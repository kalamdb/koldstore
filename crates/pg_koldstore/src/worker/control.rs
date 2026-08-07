//! Foreground validation for event-driven KoldStore maintenance.
//!
//! Client backends never register, pause, inspect, or wait for maintenance
//! processes. Capture activation only validates that automatic maintenance is
//! enabled and publishes a transaction-coalesced scheduling event after commit.

/// Requires automatic async maintenance before capture activation and records a
/// post-commit scheduling event for the current database.
pub(crate) fn require_async_mirror_worker() -> Result<(), String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Err(
            "async mirror capture requires koldstore.internal_async_mirror_worker=on".to_string(),
        );
    }
    crate::worker::wake::mark_schedule_pending();
    Ok(())
}
