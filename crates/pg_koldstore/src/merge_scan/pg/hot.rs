//! Hot heap load for KoldMergeScan via SPI.
//!
//! Two paths:
//! - **Native** (hot-only after cold prune): project selected columns as Datums
//!   with no JSON encode/decode.
//! - **JSON** (hot+cold merge): build a row image for Rust winner resolution.

use std::ffi::CStr;

use koldstore_common::{
    quote_ident, ColumnRef, HotRow, LogicalPk, PkColumn, QualifiedTableName, SeqId,
};
use koldstore_merge::scan::HOT_SEQ_SENTINEL;
use pgrx::pg_sys;

use super::qual::ScanProjection;
use super::spi_query::with_read_query;
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

/// Inequality predicates on immutable primary-key columns that may be pushed
/// into the hot JSON merge reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HotRangeFilter {
    /// Column name on the hot relation.
    pub(super) column: String,
    /// PostgreSQL comparison operator reconstructed from the query qual.
    pub(super) operator: HotRangeOperator,
    /// SQL literal already typed for the column (for example `10`).
    pub(super) sql_literal: String,
}

/// A comparison direction supported by the hot merge reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HotRangeOperator {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

impl HotRangeOperator {
    const fn sql(self) -> &'static str {
        match self {
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
        }
    }
}

/// Returns true when equality filters cover every primary-key column.
pub(super) fn equality_covers_primary_key(
    filters: &[HotEqualityFilter],
    primary_key_columns: &[ColumnRef],
) -> bool {
    !primary_key_columns.is_empty()
        && primary_key_columns.iter().all(|primary_key| {
            filters
                .iter()
                .any(|filter| filter.column.eq_ignore_ascii_case(&primary_key.name))
        })
}

/// Maximum hot JSON rows retained in one MergeStream SPI page.
pub(super) const HOT_MERGE_BATCH_ROWS: usize = 1024;

/// Paged SPI reader for merge-path hot JSON rows.
///
/// Pages are ordered by primary-key columns. After the first page, later pages
/// use a keyset predicate `(pk…) > (last…)` instead of `OFFSET` so PostgreSQL
/// does not re-scan already-emitted rows. Application-table PK uniqueness makes
/// cross-page hot duplicates impossible.
#[derive(Debug)]
pub(super) struct HotMergeBatchReader {
    /// Base SELECT … ORDER BY SQL without LIMIT / keyset predicate.
    ordered_sql: String,
    pk_columns: Vec<PkColumn>,
    batch_size: usize,
    /// Exclusive lower bound for the next page (`None` = first page).
    after_pk: Option<LogicalPk>,
    exhausted: bool,
    relation_owner: pg_sys::Oid,
}

impl HotMergeBatchReader {
    /// Builds a reader that yields empty pages (cold-only merge / point paths).
    #[must_use]
    pub(super) fn empty(relation_owner: pg_sys::Oid) -> Self {
        Self {
            ordered_sql: String::new(),
            pk_columns: Vec::new(),
            batch_size: HOT_MERGE_BATCH_ROWS,
            after_pk: None,
            exhausted: true,
            relation_owner,
        }
    }

    /// Prepares a paged hot JSON reader without fetching the first page.
    pub(super) fn open(
        relation: &str,
        snapshot: &koldstore_catalog::ManagedTableSnapshot,
        equality_filters: &[HotEqualityFilter],
        range_filters: &[HotRangeFilter],
        projected_columns: &[&koldstore_migrate::order::CatalogColumn],
        relation_owner: pg_sys::Oid,
    ) -> Result<Self, String> {
        let table = QualifiedTableName::parse(relation).map_err(|error| error.to_string())?;
        let pk_columns = snapshot
            .primary_key_columns
            .iter()
            .map(|column| PkColumn::new(&column.name).map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        if pk_columns.is_empty() {
            return Err("managed table primary key is required for hot merge paging".to_string());
        }

        let mut select_names: Vec<String> = projected_columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        for pk in &snapshot.primary_key_columns {
            if !select_names.iter().any(|name| name == &pk.name) {
                select_names.push(pk.name.clone());
            }
        }
        let select_list = select_names
            .iter()
            .map(|name| format!("hot.{}", quote_ident(name)))
            .collect::<Vec<_>>()
            .join(", ");
        let hot_pk = super::spi_query::jsonb_pk_object_pairs(
            "proj",
            snapshot
                .primary_key_columns
                .iter()
                .map(|column| column.name.as_str()),
        );
        let where_clause = where_clause_sql(equality_filters, range_filters);
        let ordered_sql = format!(
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

        Ok(Self {
            ordered_sql,
            pk_columns,
            batch_size: HOT_MERGE_BATCH_ROWS,
            after_pk: None,
            exhausted: false,
            relation_owner,
        })
    }

    /// Fetches the next hot page under the relation-owner merge identity.
    ///
    /// Returns `Ok(None)` when every visible hot row has already been read.
    pub(super) fn next_batch(&mut self) -> Result<Option<Vec<HotRow>>, String> {
        if self.exhausted {
            return Ok(None);
        }
        let mut sql = self.ordered_sql.clone();
        if let Some(after) = &self.after_pk {
            let predicate = super::keyset::keyset_after_predicate("proj", &self.pk_columns, after)?;
            sql.push_str(" WHERE ");
            sql.push_str(&predicate);
        }
        sql.push_str(&format!(
            " ORDER BY {order_by} LIMIT {limit}",
            order_by = self
                .pk_columns
                .iter()
                .map(|column| format!("proj.{}", quote_ident(column.as_str())))
                .collect::<Vec<_>>()
                .join(", "),
            limit = self.batch_size,
        ));
        let rows =
            crate::catalog::owner::with_relation_owner_for_merge(self.relation_owner, || {
                with_hook_disabled(|| unsafe { execute_hot_rows_query(&sql, &self.pk_columns) })
            })?;
        let fetched = rows.len();
        if fetched < self.batch_size {
            self.exhausted = true;
        }
        if fetched == 0 {
            Ok(None)
        } else {
            // Exclusive lower bound for the next page is the last emitted PK.
            self.after_pk = rows.last().map(|row| row.pk.clone());
            Ok(Some(rows))
        }
    }

    /// Rewinds paging for PostgreSQL rescan without dropping prepared SQL.
    pub(super) fn reset(&mut self) {
        self.after_pk = None;
        self.exhausted = self.ordered_sql.is_empty();
    }
}

/// Loads projected hot columns as native Datums when cold storage is fully pruned.
///
/// PERFORMANCE: skips `to_jsonb` / `jsonb_build_object` and JSON parse on the
/// hot-only path used for PK lookups that miss every cold segment.
///
/// Plans such as `SELECT count(*)` reference no user Vars, so `projected_columns`
/// may be empty. Those scans still need one ExecScan tuple per visible heap row.
pub(super) fn load_hot_rows_native(
    relation: &str,
    equality_filters: &[HotEqualityFilter],
    projected_columns: &[&koldstore_migrate::order::CatalogColumn],
    scan_projection: &ScanProjection,
    memory: &mut ScanMemory,
) -> Result<Vec<MaterializedRow>, String> {
    let table = QualifiedTableName::parse(relation).map_err(|error| error.to_string())?;
    let where_clause = where_clause_sql(equality_filters, &[]);
    if projected_columns.is_empty() {
        let sql = format!(
            "SELECT 1 FROM ONLY {table} AS hot {where_clause}",
            table = table.quoted(),
        );
        return with_hook_disabled(|| unsafe { execute_hot_row_placeholders(&sql) });
    }
    let select_list = projected_columns
        .iter()
        .map(|column| format!("hot.{}", quote_ident(&column.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {select_list} FROM ONLY {table} AS hot {where_clause}",
        table = table.quoted(),
    );

    with_hook_disabled(|| unsafe { execute_hot_rows_native(&sql, scan_projection, memory) })
}

fn where_clause_sql(
    equality_filters: &[HotEqualityFilter],
    range_filters: &[HotRangeFilter],
) -> String {
    if equality_filters.is_empty() && range_filters.is_empty() {
        return String::new();
    }
    let mut predicates = equality_filters
        .iter()
        .map(|filter| {
            format!(
                "hot.{column} = {literal}",
                column = quote_ident(&filter.column),
                literal = filter.sql_literal
            )
        })
        .collect::<Vec<_>>();
    predicates.extend(range_filters.iter().map(|filter| {
        format!(
            "hot.{column} {operator} {literal}",
            column = quote_ident(&filter.column),
            operator = filter.operator.sql(),
            literal = filter.sql_literal
        )
    }));
    format!("WHERE {}", predicates.join(" AND "))
}

unsafe fn execute_hot_rows_query(
    query: &str,
    pk_columns: &[PkColumn],
) -> Result<Vec<HotRow>, String> {
    with_read_query(query, |processed, tuptable| {
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
                rows.push(HotRow {
                    pk,
                    scope_key: None,
                    seq,
                    deleted: false,
                    row_image,
                });
            }
        }
        Ok(rows)
    })
}

/// Counts visible hot rows without projecting attributes (for `count(*)` etc.).
unsafe fn execute_hot_row_placeholders(query: &str) -> Result<Vec<MaterializedRow>, String> {
    with_read_query(query, |processed, _| {
        Ok((0..processed)
            .map(|_| MaterializedRow {
                values: Vec::new(),
                is_null: Vec::new(),
            })
            .collect())
    })
}

unsafe fn execute_hot_rows_native(
    query: &str,
    scan_projection: &ScanProjection,
    memory: &mut ScanMemory,
) -> Result<Vec<MaterializedRow>, String> {
    with_read_query(query, |processed, tuptable| {
        let mut rows = Vec::with_capacity(processed);
        if !tuptable.is_null() {
            let tupdesc = (*tuptable).tupdesc;
            let type_meta = column_type_meta(tupdesc, scan_projection.columns.len())?;
            for index in 0..processed {
                let tuple = *(*tuptable).vals.add(index);
                let row = memory.switch(|| {
                    materialize_spi_tuple(tuple, tupdesc, &type_meta, scan_projection)
                })?;
                rows.push(row);
            }
        }
        Ok(rows)
    })
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
    let mut meta = Vec::with_capacity(column_count);
    for index in 0..column_count {
        meta.push(column_type_meta_at(tupdesc, natts, index)?);
    }
    Ok(meta)
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
unsafe fn column_type_meta_at(
    tupdesc: pg_sys::TupleDesc,
    natts: usize,
    index: usize,
) -> Result<ColumnTypeMeta, String> {
    let attr = &(*tupdesc).attrs.as_slice(natts)[index];
    let mut typlen: i16 = 0;
    let mut typbyval = false;
    let mut typalign: std::os::raw::c_char = 0;
    pg_sys::get_typlenbyvalalign(attr.atttypid, &mut typlen, &mut typbyval, &mut typalign);
    Ok(ColumnTypeMeta { typlen, typbyval })
}

#[cfg(feature = "pg18")]
unsafe fn column_type_meta_at(
    tupdesc: pg_sys::TupleDesc,
    natts: usize,
    index: usize,
) -> Result<ColumnTypeMeta, String> {
    let attr = &(*tupdesc).compact_attrs.as_slice(natts)[index];
    Ok(ColumnTypeMeta {
        typlen: attr.attlen,
        typbyval: attr.attbyval,
    })
}

unsafe fn materialize_spi_tuple(
    tuple: pg_sys::HeapTuple,
    tupdesc: pg_sys::TupleDesc,
    type_meta: &[ColumnTypeMeta],
    scan_projection: &ScanProjection,
) -> Result<MaterializedRow, String> {
    let mut values = Vec::with_capacity(scan_projection.columns.len());
    let mut is_null = Vec::with_capacity(scan_projection.columns.len());
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
