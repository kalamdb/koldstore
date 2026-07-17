//! Pure ensure-state decisions for database-scoped workers.
//!
//! The PostgreSQL adapter supplies whether `pg_stat_activity` currently shows
//! the worker. This module only decides the next action so unit tests need no
//! Postgres. Backend-local "already kicked" fast paths stay in the adapter.

/// Action the adapter should take after evaluating ensure state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsureAction {
    /// Worker is already running; leave the fast path marked ensured.
    AlreadyRunning,
    /// Clear the fast path and register a new dynamic background worker.
    Register,
}

/// Returns the idempotent next action for a live activity probe.
///
/// Invariants:
/// - `running` → skip registration (idempotent kick / another backend started it).
/// - `!running` → register (covers first start and crash recovery).
#[must_use]
pub const fn ensure_action(running: bool) -> EnsureAction {
    if running {
        EnsureAction::AlreadyRunning
    } else {
        EnsureAction::Register
    }
}

#[cfg(test)]
mod tests {
    use super::{ensure_action, EnsureAction};

    #[test]
    fn running_skips_registration() {
        assert_eq!(ensure_action(true), EnsureAction::AlreadyRunning);
    }

    #[test]
    fn dead_registers() {
        assert_eq!(ensure_action(false), EnsureAction::Register);
    }
}
