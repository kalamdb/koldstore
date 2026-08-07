//! Compatibility shims for the former session-side WAL-worker ensure API.
//!
//! Production backends never call RegisterDynamicBackgroundWorker. They mark
//! transaction-local maintenance intent; a successful top-level commit publishes
//! the shared event and wakes the single cluster supervisor.

use koldstore_worker::DatabaseOid;

/// Legacy compatibility hook; there is no backend-local WORKER_ENSURED state anymore.
pub(crate) fn mark_worker_not_ensured() {}

/// Requests maintenance for the current database after the surrounding commit.
///
/// The function deliberately does not wait for process startup and does not
/// inspect pg_stat_activity. If the caller rolls back, no worker is dispatched.
pub(crate) fn ensure_async_mirror_worker() -> Result<bool, String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Ok(false);
    }
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    if crate::worker::wake::ensure_paused(database_oid) {
        return Ok(false);
    }
    crate::worker::wake::mark_schedule_pending();
    Ok(true)
}

/// Compatibility form used by old callers that already know a database OID.
///
/// Cross-database calls cannot be represented by a current-backend transaction
/// dirty bit, so this form is only safe for supervisor/recovery code and directly
/// publishes a durable recovery hint. Normal client code uses the no-argument form.
pub(crate) fn ensure_async_mirror_worker_for(database_oid: DatabaseOid) -> Result<bool, String> {
    if !crate::guc::async_mirror_worker_enabled()
        || crate::worker::wake::ensure_paused(database_oid.get())
    {
        return Ok(false);
    }
    crate::worker::wake::request_recovery(database_oid.get());
    Ok(true)
}

/// Former once-per-backend ensure path.
pub(crate) fn ensure_async_mirror_worker_once_if_needed() {
    let _ = ensure_async_mirror_worker();
}

/// Requires automatic async maintenance to be enabled before capture activation.
///
/// The activation transaction marks scheduling dirty. The worker is dispatched
/// only after publication/catalog activation commits, so it cannot race ahead of
/// an uncommitted manage_table transaction.
pub(crate) fn require_async_mirror_worker() -> Result<(), String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Err(
            "async mirror capture requires koldstore.internal_async_mirror_worker=on".to_string(),
        );
    }
    crate::worker::wake::mark_schedule_pending();
    Ok(())
}

/// Internal compatibility SQL entry point used by existing tests/diagnostics.
///
/// It now requests supervisor maintenance at transaction commit rather than
/// registering a process in this client backend.
#[pgrx::pg_extern(
    name = "internal_ensure_async_mirror_worker",
    schema = "koldstore",
    security_definer
)]
pub fn ensure_async_mirror_worker_pg() -> bool {
    ensure_async_mirror_worker()
        .unwrap_or_else(|error| pgrx::error!("could not request async mirror maintenance: {error}"))
}

/// Test/benchmark compatibility control that pauses supervisor maintenance
/// dispatch for the current database. Production scheduling does not use it.
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
        crate::worker::wake::mark_schedule_pending();
        true
    }
}
