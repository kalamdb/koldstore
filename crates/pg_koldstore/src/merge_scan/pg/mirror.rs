//! Mirror overlay for KoldMergeScan merge reads.
//!
//! Unflushed mirror tombstones (`op = 3`) mask cold Parquet rows so committed
//! deletes are invisible before flush. Live mirror rows (`op` 1/2) do not need
//! an overlay load: the hot heap already holds the current row and wins merge.

use std::collections::HashSet;
use std::ffi::CString;

use koldstore_common::{quote_ident, LogicalPk, PkColumn, TableName};
use pgrx::pg_sys;

use super::hot::HotEqualityFilter;
use super::with_hook_disabled;

/// Mirror tombstones that must mask cold Parquet rows until flush.
#[derive(Debug, Default, Clone)]
pub(super) struct MirrorOverlay {
    /// Unflushed tombstone PKs (`op = 3`), keyed as [`LogicalPk`].
    pub masked_pks: HashSet<LogicalPk>,
    /// Count of tombstone (op = 3) rows in the overlay.
    pub tombstones: usize,
    /// Live overrides are not loaded; kept for EXPLAIN compatibility (always 0).
    pub live_overrides: usize,
}

impl MirrorOverlay {
    /// Returns true when cold state for this PK must be skipped.
    #[must_use]
    pub(super) fn masks_pk(&self, pk: &LogicalPk) -> bool {
        self.masked_pks.contains(pk)
    }

    #[must_use]
    pub(super) fn is_empty(&self) -> bool {
        self.masked_pks.is_empty()
    }
}

/// Loads mirror tombstones that can mask cold rows for this scan.
///
/// When `pk_filters` contains primary-key equality predicates, only those keys
/// are probed (point-lookup path). Otherwise all `op = 3` rows are loaded.
///
/// Live `op` 1/2 rows are intentionally omitted: hot heap state already wins.
pub(super) fn load_mirror_tombstone_overlay(
    mirror_relation: &TableName,
    primary_key_columns: &[String],
    pk_filters: &[HotEqualityFilter],
) -> Result<MirrorOverlay, String> {
    if primary_key_columns.is_empty() {
        return Err("mirror overlay requires primary key columns".to_string());
    }
    let pk_columns = primary_key_columns
        .iter()
        .map(|column| PkColumn::new(column.as_str()).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let pk_json = primary_key_columns
        .iter()
        .map(|column| {
            format!(
                "'{escaped}', mirror.{quoted}",
                escaped = column.replace('\'', "''"),
                quoted = quote_ident(column),
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut where_clauses = vec!["mirror.\"op\" = 3".to_string()];
    let pk_filter_names: HashSet<&str> = primary_key_columns.iter().map(String::as_str).collect();
    let applicable: Vec<&HotEqualityFilter> = pk_filters
        .iter()
        .filter(|filter| pk_filter_names.contains(filter.column.as_str()))
        .collect();
    // Point lookup: only probe the requested PK columns when we have a full PK.
    if !applicable.is_empty() && applicable.len() == primary_key_columns.len() {
        for filter in applicable {
            where_clauses.push(format!(
                "mirror.{} = {}",
                quote_ident(&filter.column),
                filter.sql_literal
            ));
        }
    }

    let sql = format!(
        r#"
SELECT
    jsonb_build_object({pk_json})::text AS pk_json,
    mirror."op" AS op
FROM {mirror} AS mirror
WHERE {where_clause}
"#,
        pk_json = pk_json,
        mirror = mirror_relation.quoted(),
        where_clause = where_clauses.join(" AND "),
    );

    with_hook_disabled(|| unsafe { execute_mirror_overlay_query(&sql, &pk_columns) })
}

unsafe fn execute_mirror_overlay_query(
    query: &str,
    pk_columns: &[PkColumn],
) -> Result<MirrorOverlay, String> {
    let query = CString::new(query).map_err(|error| error.to_string())?;
    let connect = pg_sys::SPI_connect();
    if connect < 0 {
        return Err(format!("SPI_connect failed with code {connect}"));
    }
    let execute = pg_sys::SPI_execute(query.as_ptr(), true, 0);
    if execute < 0 {
        let _ = pg_sys::SPI_finish();
        return Err(format!("SPI_execute failed with code {execute}"));
    }

    let processed = usize::try_from(pg_sys::SPI_processed).map_err(|error| error.to_string())?;
    let tuptable = pg_sys::SPI_tuptable;
    let mut overlay = MirrorOverlay::default();
    if !tuptable.is_null() {
        let tupdesc = (*tuptable).tupdesc;
        for index in 0..processed {
            let tuple = *(*tuptable).vals.add(index);
            let pk_json_text = spi_text(tuple, tupdesc, 1)?;
            let pk_value: serde_json::Value = serde_json::from_str(&pk_json_text)
                .map_err(|error| format!("mirror overlay pk JSON: {error}"))?;
            let pk = LogicalPk::from_json_object(&pk_value, pk_columns)
                .map_err(|error| error.to_string())?;
            overlay.tombstones += 1;
            overlay.masked_pks.insert(pk);
        }
    }

    let finish = pg_sys::SPI_finish();
    if finish < 0 {
        return Err(format!("SPI_finish failed with code {finish}"));
    }
    Ok(overlay)
}

unsafe fn spi_text(
    tuple: pg_sys::HeapTuple,
    tupdesc: pg_sys::TupleDesc,
    attno: i32,
) -> Result<String, String> {
    let mut isnull = false;
    let datum = pg_sys::SPI_getbinval(tuple, tupdesc, attno, &mut isnull);
    if isnull {
        return Err(format!("mirror overlay column {attno} is null"));
    }
    let cstr = pg_sys::SPI_getvalue(tuple, tupdesc, attno);
    if cstr.is_null() {
        let _ = datum;
        return Err(format!("mirror overlay column {attno} text is null"));
    }
    Ok(std::ffi::CStr::from_ptr(cstr)
        .to_string_lossy()
        .into_owned())
}
