//! Async mirror status SQL surface for lag and apply-rate observability.

use pgrx::datum::DatumWithOid;
use serde_json::json;

use super::lifecycle::current_slot_name;

/// Returns async mirror lag, retained WAL, and process-local apply rates.
///
/// SQL contract: `koldstore.async_mirror_status()` → `jsonb`. Never drops WAL;
/// callers use this to alert and tune the retained-WAL health threshold.
#[pgrx::pg_extern(name = "async_mirror_status", schema = "koldstore")]
pub fn async_mirror_status() -> pgrx::JsonB {
    pgrx::JsonB(
        async_mirror_status_impl()
            .unwrap_or_else(|error| json!({ "error": error, "healthy": false })),
    )
}

fn async_mirror_status_impl() -> Result<serde_json::Value, String> {
    let slot = current_slot_name();
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId };
    let metrics = crate::observability::async_apply_metrics();

    // Prefer CAST(... AS text) over `expr::text`: this SPI path failed with
    // `syntax error at or near "."` when using `::` casts on nested
    // jsonb_build_object results.
    let slot_row = pgrx::Spi::get_one_with_args::<String>(
        "SELECT COALESCE(\
           (SELECT CAST(jsonb_build_object(\
              'slot_name', slot_name,\
              'active', active,\
              'confirmed_flush_lsn', CAST(confirmed_flush_lsn AS text),\
              'retained_bytes', pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)\
            ) AS text)\
            FROM pg_catalog.pg_replication_slots WHERE slot_name = $1), \
           CAST(jsonb_build_object('slot_name', $1, 'present', false) AS text)\
         )",
        &[DatumWithOid::from(slot.as_str())],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| json!({ "slot_name": slot, "present": false }).to_string());

    let state_row = pgrx::Spi::get_one_with_args::<String>(
        "SELECT COALESCE(\
           (SELECT CAST(jsonb_build_object(\
              'applied_lsn', CAST(applied_lsn AS text),\
              'updated_at', updated_at,\
              'updated_at_age_seconds', EXTRACT(EPOCH FROM (now() - updated_at))\
            ) AS text)\
            FROM koldstore.async_mirror_state WHERE database_oid = $1), \
           CAST(jsonb_build_object('present', false) AS text)\
         )",
        &[DatumWithOid::from(database_oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| json!({ "present": false }).to_string());

    let slot_json: serde_json::Value =
        serde_json::from_str(&slot_row).map_err(|error| error.to_string())?;
    let state_json: serde_json::Value =
        serde_json::from_str(&state_row).map_err(|error| error.to_string())?;

    let retained_bytes = slot_json
        .get("retained_bytes")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    let max_retained = crate::guc::async_mirror_max_retained_bytes();
    let retained_wal_within_threshold = max_retained <= 0 || retained_bytes <= max_retained;
    let retention_health = json!({
        "max_retained_bytes": max_retained,
        "retained_bytes": retained_bytes,
        "ok": retained_wal_within_threshold,
    });

    Ok(json!({
        "slot": slot_json,
        "state": state_json,
        "apply": {
            "rows_total": metrics.rows_total,
            "ticks_total": metrics.ticks_total,
            "last_rows": metrics.last_rows,
            "last_elapsed_ms": metrics.last_elapsed_ms,
            "error_total": metrics.error_total,
            "rate_rows_per_sec": if metrics.last_elapsed_ms > 0 {
                (metrics.last_rows as f64) * 1000.0 / (metrics.last_elapsed_ms as f64)
            } else {
                0.0
            },
        },
        "retention": retention_health.clone(),
        // Compatibility alias for clients written before the threshold became
        // health-only. New consumers should read `retention`.
        "admission": retention_health,
        "healthy": metrics.healthy && retained_wal_within_threshold,
    }))
}
