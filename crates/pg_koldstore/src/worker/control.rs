//! Foreground controls for the event-driven KoldStore supervisor.
//!
//! Client backends never register or wait for worker processes. They validate
//! capture configuration and publish transaction-coalesced events that the
//! cluster supervisor consumes after commit. Test/diagnostic pause SQL exists so
//! e2e can hold a database idle without advisory locks.

/// Requires automatic async capture before table activation.
///
/// The logical slot is provisioned outside ordinary transactional DDL, so the
/// database-level WAL service requirement may also be published immediately.
/// The WAL generation remains commit-aware: only successful activation wakes
/// the supervisor and starts the process, ensuring the first application write
/// never pays dynamic-process startup latency.
pub(crate) fn require_async_mirror_worker() -> Result<(), String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Err(
            "async mirror capture requires koldstore.internal_async_mirror_worker=on".to_string(),
        );
    }
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    if !crate::worker::wal::require(database_oid) {
        return Err("KoldStore WAL-applier registry is full".to_string());
    }
    crate::worker::wake::mark_managed_dml_pending();
    crate::worker::wake::mark_schedule_pending();
    Ok(())
}

/// Internal diagnostic SQL entry point. It publishes WAL and maintenance work
/// but never registers a dynamic process or inspects `pg_stat_activity` from the
/// client backend.
#[pgrx::pg_extern(
    name = "internal_ensure_async_mirror_worker",
    schema = "koldstore",
    security_definer
)]
pub fn request_async_mirror_maintenance_pg() -> bool {
    if !crate::guc::async_mirror_worker_enabled() {
        return false;
    }
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    if crate::worker::wake::ensure_paused(database_oid)
        || !crate::worker::wal::require(database_oid)
    {
        return false;
    }
    crate::worker::wake::mark_managed_dml_pending();
    crate::worker::wake::mark_schedule_pending();
    true
}

/// Test/benchmark control that pauses supervisor WAL/maintenance dispatch for the
/// current database. Production scheduling does not use it.
#[pgrx::pg_extern(
    name = "internal_set_async_mirror_ensure_paused",
    schema = "koldstore",
    security_definer
)]
pub fn set_async_mirror_ensure_paused_pg(paused: bool) -> bool {
    let oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    if paused {
        if !crate::worker::wake::pause_ensure(oid) {
            pgrx::error!("async mirror maintenance pause set is full");
        }
        true
    } else {
        crate::worker::wake::resume_ensure(oid);
        let _ = crate::worker::wal::require(oid);
        crate::worker::wake::request_recovery(oid);
        true
    }
}
