//! Async mirror status SQL surface for lag, WAL service, scheduling, and apply rates.

use pgrx::datum::DatumWithOid;
use serde_json::json;

use super::lifecycle::current_slot_name;

/// Returns async mirror lag, WAL watermarks, worker state, slot identity, and
/// apply rates.
#[pgrx::pg_extern(name = "async_mirror_status", schema = "koldstore")]
pub fn async_mirror_status() -> pgrx::JsonB {
    pgrx::JsonB(
        async_mirror_status_value()
            .unwrap_or_else(|error| serde_json::json!({ "error": error, "healthy": false })),
    )
}

/// Builds the async mirror status JSON (shared by SQL and `table_status`).
pub(crate) fn async_mirror_status_value() -> Result<serde_json::Value, String> {
    async_mirror_status_impl()
}

fn async_mirror_status_impl() -> Result<serde_json::Value, String> {
    use koldstore_catalog::queries::{
        plan_async_mirror_slot_status, plan_async_mirror_state_status,
    };

    let slot = current_slot_name();
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId };
    let database_oid_u32 = database_oid.to_u32();
    let metrics = crate::observability::async_apply_metrics();

    let slot_plan = plan_async_mirror_slot_status().map_err(|error| error.to_string())?;
    let slot_row = pgrx::Spi::get_one_with_args::<String>(
        &slot_plan.sql,
        &[DatumWithOid::from(slot.as_str())],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| {
        json!({
            "slot_name": slot,
            "present": false,
        })
        .to_string()
    });

    let state_plan = plan_async_mirror_state_status().map_err(|error| error.to_string())?;
    let state_row = pgrx::Spi::get_one_with_args::<String>(
        &state_plan.sql,
        &[DatumWithOid::from(database_oid)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or_else(|| json!({ "present": false }).to_string());

    let slot_json: serde_json::Value =
        serde_json::from_str(&slot_row).map_err(|error| error.to_string())?;
    let state_json: serde_json::Value =
        serde_json::from_str(&state_row).map_err(|error| error.to_string())?;

    let current_wal_lsn = slot_json
        .get("current_wal_lsn")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let confirmed_flush_lsn = slot_json
        .get("confirmed_flush_lsn")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let applied_lsn = state_json
        .get("applied_lsn")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
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

    let shared = crate::worker::wake::supervisor_snapshot(database_oid_u32);
    let applier = crate::worker::wal::snapshot(database_oid_u32);
    let wal_pending = shared.is_some_and(|snapshot| {
        snapshot.wal_generation != snapshot.wal_processed_generation
            || snapshot.event_flags & koldstore_worker::EVENT_RECOVERY_REQUIRED != 0
    });
    let wal_applier = json!({
        "registered": applier.is_some(),
        "pid": applier.and_then(|state| (state.pid > 0).then_some(state.pid)),
        "running": applier.is_some_and(|state| state.running()),
        "starting": applier.is_some_and(|state| state.starting()),
        "pending": wal_pending,
        "wal_generation": shared.map(|snapshot| snapshot.wal_generation).unwrap_or(0),
        "wal_processed_generation": shared
            .map(|snapshot| snapshot.wal_processed_generation)
            .unwrap_or(0),
        "watchdog_ms": 30_000,
    });
    let wal_service_healthy = !wal_pending
        || applier.is_some_and(|state| state.running() || state.starting());

    // Compatibility: existing operators/tests read the composite `maintenance`
    // object. Keep WAL generation fields there while exposing the new process
    // boundary explicitly under `wal_applier`.
    let maintenance = shared
        .map(|snapshot| {
            let maintenance_pending =
                snapshot.maintenance_generation != snapshot.maintenance_processed_generation;
            json!({
                "registered": true,
                "pid": (snapshot.maintenance_pid > 0).then_some(snapshot.maintenance_pid),
                "running": snapshot.maintenance_pid > 0,
                "starting": snapshot.maintenance_pid < 0,
                "pending": wal_pending || maintenance_pending,
                "wal_generation": snapshot.wal_generation,
                "wal_processed_generation": snapshot.wal_processed_generation,
                "maintenance_generation": snapshot.maintenance_generation,
                "maintenance_processed_generation": snapshot.maintenance_processed_generation,
                "recovery_requested": snapshot.event_flags & koldstore_worker::EVENT_RECOVERY_REQUIRED != 0,
                "schedule_dirty": snapshot.event_flags & koldstore_worker::EVENT_SCHEDULE_DIRTY != 0,
                "next_due_at_ms": snapshot.next_maintenance_due_at_ms,
            })
        })
        .unwrap_or_else(|| {
            json!({
                "registered": false,
                "pid": serde_json::Value::Null,
                "running": false,
                "starting": false,
                "pending": false,
                "wal_generation": 0,
                "wal_processed_generation": 0,
                "maintenance_generation": 0,
                "maintenance_processed_generation": 0,
                "recovery_requested": false,
                "schedule_dirty": false,
                "next_due_at_ms": 0,
            })
        });

    let wal = json!({
        "current_lsn": current_wal_lsn,
        "applied_lsn": applied_lsn,
        "confirmed_flush_lsn": confirmed_flush_lsn,
        "restart_lsn": slot_json.get("restart_lsn").cloned().unwrap_or(serde_json::Value::Null),
        "lag_bytes": retained_bytes,
    });

    Ok(json!({
        "slot_name": slot,
        "slot": slot_json,
        "state": state_json,
        "wal": wal,
        "wal_applier": wal_applier,
        "maintenance": maintenance,
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
        "retention": retention_health,
        "healthy": metrics.healthy && retained_wal_within_threshold && wal_service_healthy,
    }))
}
