//! Async mirror status SQL surface for lag, WAL service, scheduling, and apply rates.

use koldstore_supervisor::EVENT_RECOVERY_REQUIRED;
use koldstore_wal_mirror::{
    build_async_mirror_status, ApplyMetricsSnapshot, StatusSupervisorSnapshot,
    StatusWalApplierSnapshot,
};
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

    let shared = crate::worker::wake::supervisor_snapshot(database_oid_u32).map(|snapshot| {
        StatusSupervisorSnapshot {
            wal_generation: snapshot.wal_generation,
            wal_processed_generation: snapshot.wal_processed_generation,
            maintenance_generation: snapshot.maintenance_generation,
            maintenance_processed_generation: snapshot.maintenance_processed_generation,
            maintenance_pid: snapshot.maintenance_pid,
            next_maintenance_due_at_ms: snapshot.next_maintenance_due_at_ms,
            recovery_requested: snapshot.event_flags & EVENT_RECOVERY_REQUIRED != 0,
            schedule_dirty: snapshot.event_flags & koldstore_supervisor::EVENT_SCHEDULE_DIRTY != 0,
        }
    });
    let applier = crate::worker::wal::snapshot(database_oid_u32);
    let wal_required = applier.is_some_and(|state| state.required);
    let wal_live = applier.is_some_and(|state| {
        state.required && crate::worker::wal::process_alive(database_oid_u32, state.pid)
    });
    let wal_starting = applier.is_some_and(|state| state.required && state.starting());
    let wal_applier = StatusWalApplierSnapshot {
        required: wal_required,
        pid: applier.and_then(|state| {
            (state.required && (wal_live || state.starting())).then_some(state.pid)
        }),
        running: wal_live,
        starting: wal_starting,
    };

    Ok(build_async_mirror_status(
        &slot,
        slot_json,
        state_json,
        crate::guc::async_mirror_max_retained_bytes(),
        shared,
        wal_applier,
        ApplyMetricsSnapshot {
            rows_total: metrics.rows_total,
            ticks_total: metrics.ticks_total,
            last_rows: metrics.last_rows,
            last_elapsed_ms: metrics.last_elapsed_ms,
            error_total: metrics.error_total,
            healthy: metrics.healthy,
        },
        30_000,
    ))
}
