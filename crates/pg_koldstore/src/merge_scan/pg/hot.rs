//! Hot heap load for KoldMergeScan via SPI.
//!
//! Two paths:
//! - **Native** (hot-only after cold prune): project selected columns as Datums
//!   with no JSON encode/decode.
//! - **JSON** (hot+cold merge): build a row image for Rust winner resolution.

use std::ffi::{CStr, CString};

use koldstore_common::{
    quote_ident, CommitSeq, HotRow, LogicalPk, PkColumn, QualifiedTableName, SeqId,
};
use koldstore_merge::scan::HOT_SEQ_SENTINEL;
use pgrx::pg_sys;

use super::tuple::{MaterializedRow, ScanMemory};
use super::with_hook_disabled;

/// Equality predicates that can be pushed into the hot heap SPI load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HotEqualityFilter {
    /// Column name on the hot relation.
    pub column: String,
    /// SQL literal already typed for the column (for example `42` or `'abc'`).
    pub sql_literal: String,
}

/// Loads live hot rows as [`HotRow`] values for Rust merge resolution.
///
/// When `equality_filters` is non-empty, they are AND-ed into the SPI query so
/// point lookups do not materialize the entire hot heap.
///
/// `projected_columns` limits the JSON image to columns needed for emit/filters.
/// Uses `to_jsonb(proj)` on a subquery (not `jsonb_build_object`) so wide tables
/// stay under PostgreSQL's `FUNC_MAX_ARGS` limit.
pub(super) fn load_hot_rows_for_merge(
    relation: &str,
    snapshot: &koldstore_catalog::ManagedTableSnapshot,
    equality_filters: &[HotEqualityFilter],
    projected_columns: &[&koldstore_migrate::order::CatalogColumn],
) -> Result<Vec<HotRow>, String> {
    let table = QualifiedTableName::parse(relation).map_err(|error| error.to_string())?;
    let pk_columns = snapshot
        .primary_key_columns
        .iter()
        .map(|column| PkColumn::new(column.as_str()).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;

    // Subquery select list: projected image columns plus any PK columns missing
    // from the projection (needed for pk_json).
    let mut select_names: Vec<String> = projected_columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    for pk in &snapshot.primary_key_columns {
        if !select_names.iter().any(|name| name == pk) {
            select_names.push(pk.clone());
        }
    }
    let select_list = select_names
        .iter()
        .map(|name| format!("hot.{}", quote_ident(name)))
        .collect::<Vec<_>>()
        .join(", ");
    let hot_pk = snapshot
        .primary_key_columns
        .iter()
        .map(|column| {
            format!(
                "'{column}', proj.{quoted}",
                column = column.replace('\'', "''"),
                quoted = quote_ident(column),
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = where_clause_sql(equality_filters);
    let sql = format!(
        r#"
SELECT
    to_jsonb(proj) AS row_image,
    jsonb_build_object({hot_pk}) AS pk_json
FROM (
    SELECT {select_list}
    FROM ONLY {table} AS hot
    {where_clause}
) AS proj
"#,
        hot_pk = hot_pk,
        select_list = select_list,
        table = table.quoted(),
        where_clause = where_clause,
    );

    with_hook_disabled(|| unsafe { execute_hot_rows_query(&sql, &pk_columns) })
}

/// Loads projected hot columns as native Datums when cold storage is fully pruned.
///
/// PERFORMANCE: skips `to_jsonb` / `jsonb_build_object` and JSON parse on the
/// hot-only path used for PK lookups that miss every cold segment.
pub(super) fn load_hot_rows_native(
    relation: &str,
    equality_filters: &[HotEqualityFilter],
    projected_columns: &[&koldstore_migrate::order::CatalogColumn],
    memory: &mut ScanMemory,
) -> Result<Vec<MaterializedRow>, String> {
    if projected_columns.is_empty() {
        return Ok(Vec::new());
    }
    let table = QualifiedTableName::parse(relation).map_err(|error| error.to_string())?;
    let select_list = projected_columns
        .iter()
        .map(|column| format!("hot.{}", quote_ident(&column.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = where_clause_sql(equality_filters);
    let sql = format!(
        "SELECT {select_list} FROM ONLY {table} AS hot {where_clause}",
        table = table.quoted(),
    );

    with_hook_disabled(|| unsafe { execute_hot_rows_native(&sql, projected_columns.len(), memory) })
}

fn where_clause_sql(equality_filters: &[HotEqualityFilter]) -> String {
    if equality_filters.is_empty() {
        return String::new();
    }
    let predicates = equality_filters
        .iter()
        .map(|filter| {
            format!(
                "hot.{column} = {literal}",
                column = quote_ident(&filter.column),
                literal = filter.sql_literal
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!("WHERE {predicates}")
}

unsafe fn execute_hot_rows_query(
    query: &str,
    pk_columns: &[PkColumn],
) -> Result<Vec<HotRow>, String> {
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
    let mut rows = Vec::with_capacity(processed);
    if !tuptable.is_null() {
        let tupdesc = (*tuptable).tupdesc;
        for index in 0..processed {
            let tuple = *(*tuptable).vals.add(index);
            let row_image = spi_text_json(tuple, tupdesc, 1)?;
            let pk_json = spi_text_json(tuple, tupdesc, 2)?;
            let pk = LogicalPk::from_json_object(&pk_json, pk_columns)
                .map_err(|error| error.to_string())?;
            let seq = SeqId::new(HOT_SEQ_SENTINEL).map_err(|error| error.to_string())?;
            let commit_seq = CommitSeq::new(HOT_SEQ_SENTINEL).map_err(|error| error.to_string())?;
            rows.push(HotRow {
                pk,
                scope_key: None,
                seq,
                commit_seq,
                deleted: false,
                row_image,
            });
        }
    }

    let finish = pg_sys::SPI_finish();
    if finish < 0 {
        return Err(format!("SPI_finish failed with code {finish}"));
    }
    Ok(rows)
}

unsafe fn execute_hot_rows_native(
    query: &str,
    column_count: usize,
    memory: &mut ScanMemory,
) -> Result<Vec<MaterializedRow>, String> {
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
    let mut rows = Vec::with_capacity(processed);
    if !tuptable.is_null() {
        let tupdesc = (*tuptable).tupdesc;
        let type_meta = column_type_meta(tupdesc, column_count)?;
        for index in 0..processed {
            let tuple = *(*tuptable).vals.add(index);
            let row = memory.switch(|| materialize_spi_tuple(tuple, tupdesc, &type_meta))?;
            rows.push(row);
        }
    }

    let finish = pg_sys::SPI_finish();
    if finish < 0 {
        return Err(format!("SPI_finish failed with code {finish}"));
    }
    Ok(rows)
}

#[derive(Debug, Clone, Copy)]
struct ColumnTypeMeta {
    typlen: i16,
    typbyval: bool,
}

unsafe fn column_type_meta(
    tupdesc: pg_sys::TupleDesc,
    column_count: usize,
) -> Result<Vec<ColumnTypeMeta>, String> {
    let natts = usize::try_from((*tupdesc).natts).map_err(|error| error.to_string())?;
    if column_count > natts {
        return Err(format!(
            "projected {column_count} columns but SPI tupdesc has {natts}"
        ));
    }
    let attrs = (*tupdesc).attrs.as_slice(natts);
    let mut meta = Vec::with_capacity(column_count);
    for attr in attrs.iter().take(column_count) {
        let mut typlen: i16 = 0;
        let mut typbyval = false;
        let mut typalign: std::os::raw::c_char = 0;
        pg_sys::get_typlenbyvalalign(attr.atttypid, &mut typlen, &mut typbyval, &mut typalign);
        meta.push(ColumnTypeMeta { typlen, typbyval });
    }
    Ok(meta)
}

unsafe fn materialize_spi_tuple(
    tuple: pg_sys::HeapTuple,
    tupdesc: pg_sys::TupleDesc,
    type_meta: &[ColumnTypeMeta],
) -> Result<MaterializedRow, String> {
    let mut values = Vec::with_capacity(type_meta.len());
    let mut is_null = Vec::with_capacity(type_meta.len());
    for (index, meta) in type_meta.iter().enumerate() {
        let attno = (index + 1) as i32;
        let mut null_flag: bool = false;
        let datum = pg_sys::SPI_getbinval(tuple, tupdesc, attno, &mut null_flag);
        if null_flag {
            values.push(pg_sys::Datum::null());
            is_null.push(true);
            continue;
        }
        // Copy into the caller's scan AllocSet (SPI_datumTransfer uses CurrentMemoryContext).
        let owned = pg_sys::SPI_datumTransfer(datum, meta.typbyval, i32::from(meta.typlen));
        values.push(owned);
        is_null.push(false);
    }
    Ok(MaterializedRow { values, is_null })
}

/// Reads a `jsonb` (or text JSON) SPI column into `serde_json::Value`.
///
/// `SPI_getvalue` invokes the type output function, so both `jsonb` and `text`
/// columns work without an extra `::text` cast in SQL.
unsafe fn spi_text_json(
    tuple: pg_sys::HeapTuple,
    tupdesc: pg_sys::TupleDesc,
    attno: i32,
) -> Result<serde_json::Value, String> {
    let cstr = pg_sys::SPI_getvalue(tuple, tupdesc, attno);
    if cstr.is_null() {
        return Ok(serde_json::Value::Null);
    }
    let text = CStr::from_ptr(cstr)
        .to_str()
        .map_err(|error| error.to_string())?
        .to_string();
    pg_sys::pfree(cstr.cast());
    serde_json::from_str(&text).map_err(|error| error.to_string())
}
