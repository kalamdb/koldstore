//! Compatibility shims for the former session-side WAL-worker ensure API.
//!
//! Production backends never register a persistent WAL worker. They publish
//! durable/shared work and wake the single cluster supervisor, which owns all
//! maintenance/executor registration.

/// Legacy compatibility hook; there is no backend-local WORKER_ENSURED state anymore.
pub(crate) fn mark_worker_not_ensured() {}

/// Requests maintenance for the current database.
///
/// Returns true when a request was accepted. The function deliberately does
/// not wait for process startup and does not inspect pg_stat_activity.
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

/// Former once-per-backend ensure path.
///
/// This is intentionally a no-op. In particular, explicit synchronous fence
/// operations call this legacy hook from older code paths; scheduling a fresh
/// maintenance worker from there would make unrelated WAL advance immediately
/// after the explicit fence. Normal source commits and manage activation already
/// publish their own transactional supervisor generations.
pub(crate) fn ensure_async_mirror_worker_once_if_needed() {}

/// Requires automatic async maintenance to be enabled before capture activation.
pub(crate) fn require_async_mirror_worker() -> Result<(), String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Err(
            "async mirror capture requires koldstore.internal_async_mirror_worker=on".to_string(),
        );
    }
    // Publication/slot activation is transactional. Request supervisor work only
    // after that transaction commits; abort/prepare clears this dirty bit.
    crate::worker::wake::mark_schedule_pending();
    Ok(())
}

/// Internal compatibility SQL entry point used by existing tests/diagnostics.
///
/// It now schedules the supervisor rather than registering a process in this
/// client backend. New background-progress tests should be passive.
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
        crate::worker::wake::request_recovery(oid);
        true
    }
}
