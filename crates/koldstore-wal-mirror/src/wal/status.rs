//! Pure JSON composition for `async_mirror_status`.
//!
//! The extension adapter fetches slot/state SPI rows and shared-memory
//! snapshots; this module only assembles the operator-facing document.

use serde_json::{json, Value};

/// Apply-rate counters published into status JSON.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ApplyMetricsSnapshot {
    pub rows_total: i64,
    pub ticks_total: i64,
    pub last_rows: i64,
    pub last_elapsed_ms: i64,
    pub error_total: i64,
    pub healthy: bool,
}

/// Shared supervisor generations for one database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusSupervisorSnapshot {
    pub wal_generation: u64,
    pub wal_processed_generation: u64,
    pub maintenance_generation: u64,
    pub maintenance_processed_generation: u64,
    pub maintenance_pid: i32,
    pub next_maintenance_due_at_ms: i64,
    pub recovery_requested: bool,
    pub schedule_dirty: bool,
}

/// Persistent WAL-applier view for one database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusWalApplierSnapshot {
    pub required: bool,
    pub pid: Option<i32>,
    pub running: bool,
    pub starting: bool,
}

/// Builds the `async_mirror_status` JSON document from already-fetched inputs.
#[must_use]
pub fn build_async_mirror_status(
    slot_name: &str,
    slot_json: Value,
    state_json: Value,
    max_retained_bytes: i64,
    shared: Option<StatusSupervisorSnapshot>,
    wal_applier: StatusWalApplierSnapshot,
    metrics: ApplyMetricsSnapshot,
    watchdog_ms: i64,
) -> Value {
    let current_wal_lsn = slot_json
        .get("current_wal_lsn")
        .cloned()
        .unwrap_or(Value::Null);
    let confirmed_flush_lsn = slot_json
        .get("confirmed_flush_lsn")
        .cloned()
        .unwrap_or(Value::Null);
    let applied_lsn = state_json
        .get("applied_lsn")
        .cloned()
        .unwrap_or(Value::Null);
    let retained_bytes = slot_json
        .get("retained_bytes")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    let retained_wal_within_threshold =
        max_retained_bytes <= 0 || retained_bytes <= max_retained_bytes;
    let retention_health = json!({
        "max_retained_bytes": max_retained_bytes,
        "retained_bytes": retained_bytes,
        "ok": retained_wal_within_threshold,
    });

    let wal_pending = wal_applier.required
        && shared.is_some_and(|snapshot| {
            snapshot.wal_generation != snapshot.wal_processed_generation
                || snapshot.recovery_requested
        });
    let wal_applier_json = json!({
        "registered": wal_applier.required,
        "required": wal_applier.required,
        "pid": wal_applier.pid,
        "running": wal_applier.running,
        "starting": wal_applier.starting,
        "pending": wal_pending,
        "wal_generation": shared.map(|snapshot| snapshot.wal_generation).unwrap_or(0),
        "wal_processed_generation": shared
            .map(|snapshot| snapshot.wal_processed_generation)
            .unwrap_or(0),
        "watchdog_ms": watchdog_ms,
    });
    let wal_service_healthy = !wal_applier.required || wal_applier.running || wal_applier.starting;

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
                "recovery_requested": snapshot.recovery_requested,
                "schedule_dirty": snapshot.schedule_dirty,
                "next_due_at_ms": snapshot.next_maintenance_due_at_ms,
            })
        })
        .unwrap_or_else(|| {
            json!({
                "registered": false,
                "pid": Value::Null,
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
        "restart_lsn": slot_json.get("restart_lsn").cloned().unwrap_or(Value::Null),
        "lag_bytes": retained_bytes,
    });

    json!({
        "slot_name": slot_name,
        "slot": slot_json,
        "state": state_json,
        "wal": wal,
        "wal_applier": wal_applier_json,
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
    })
}
