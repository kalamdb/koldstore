//! Manifest-backed row counter SPI adapters for flush and diagnostics.

use koldstore_common::QualifiedTableName;
use koldstore_flush::{
    plan_read_table_row_counters, plan_refresh_table_row_counters, TableRowCounters,
};

/// Reads O(1) row counters from `koldstore.manifest`.
///
/// # Errors
///
/// Returns an error when the manifest row is missing or SPI fails.
pub(crate) fn read_table_row_counters(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<TableRowCounters, String> {
    use pgrx::datum::DatumWithOid;

    let statement = plan_read_table_row_counters().map_err(|error| error.to_string())?;
    let json = crate::spi::select_one::<String>(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "manifest row counters are not initialized for this table".to_string())?;
    let value: serde_json::Value =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    Ok(TableRowCounters {
        hot_row_count: value
            .get("hot_row_count")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
        mirror_row_count: value
            .get("mirror_row_count")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
        cold_row_count: value
            .get("cold_row_count")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
        cold_segment_count: value
            .get("cold_segment_count")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
    })
}

/// Reconciles manifest row counters from live heap and mirror counts.
///
/// Called after `manage_table` backfill so cached counters match reality before
/// capture-trigger bumps take over.
///
/// # Errors
///
/// Returns an error when SPI execution fails.
pub(crate) fn refresh_table_row_counters(
    table_oid: pgrx::pg_sys::Oid,
    table: &QualifiedTableName,
    mirror: &QualifiedTableName,
) -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let statement =
        plan_refresh_table_row_counters(table, mirror).map_err(|error| error.to_string())?;
    crate::spi::update(&statement, &[DatumWithOid::from(table_oid)])
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// SQL contract: `koldstore.internal_record_row_count_delta(table_oid, hot_delta, mirror_delta)`.
///
/// PERFORMANCE: Updates backend-local counter deltas only. Manifest rows are updated once per
/// touched table on transaction commit (see `row_counter_cache.rs`), not once per DML row.
#[cfg(feature = "pg")]
#[pgrx::pg_extern(name = "internal_record_row_count_delta", schema = "koldstore")]
fn internal_record_row_count_delta(
    table_oid: pgrx::pg_sys::Oid,
    hot_delta: i64,
    mirror_delta: i64,
) {
    crate::row_counter_cache::record_delta(table_oid, hot_delta, mirror_delta);
}
