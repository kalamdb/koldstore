//! Pure registration backoff and wait scheduling for the cluster supervisor.
//!
//! These helpers are PostgreSQL-free. The extension adapter owns latches, SPI
//! liveness probes, and dynamic worker registration.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::DatabaseWorkSnapshot;

/// Default safety reconcile cadence when KoldStore slots exist.
pub const SAFETY_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
/// Initial delay after a failed dynamic-worker registration.
pub const REGISTRATION_RETRY_MIN: Duration = Duration::from_millis(100);
/// Cap on registration retry backoff under sustained `max_worker_processes` pressure.
pub const REGISTRATION_RETRY_MAX: Duration = Duration::from_secs(5);

/// Kind of dynamically registered worker under supervisor control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DynamicWorkerKind {
    /// Persistent per-database WAL applier.
    Wal,
    /// Ephemeral maintenance worker.
    Maintenance,
    /// Bounded one-shot flush executor.
    Flush,
}

#[derive(Debug, Clone, Copy)]
struct RegistrationRetry {
    failures: u8,
    next_attempt_at: Instant,
}

/// Supervisor-local retry state for dynamic-worker registration pressure.
///
/// `max_worker_processes` exhaustion is not durable work failure, so retrying is
/// correct, but a fixed cluster wake burns CPU indefinitely under sustained
/// pressure. Backoff lives only in the static supervisor; durable generations
/// remain dirty and survive supervisor/postmaster restart independently.
#[derive(Debug, Default)]
pub struct RegistrationBackoff {
    entries: HashMap<(u32, DynamicWorkerKind), RegistrationRetry>,
}

impl RegistrationBackoff {
    /// Returns whether registration for `(database_oid, kind)` may be attempted.
    #[must_use]
    pub fn ready(&self, database_oid: u32, kind: DynamicWorkerKind, now: Instant) -> bool {
        self.entries
            .get(&(database_oid, kind))
            .is_none_or(|retry| now >= retry.next_attempt_at)
    }

    /// Clears backoff after a successful registration.
    pub fn succeeded(&mut self, database_oid: u32, kind: DynamicWorkerKind) {
        self.entries.remove(&(database_oid, kind));
    }

    /// Records a registration failure and returns the chosen retry delay.
    pub fn failed(&mut self, database_oid: u32, kind: DynamicWorkerKind, now: Instant) -> Duration {
        let failures = self
            .entries
            .get(&(database_oid, kind))
            .map(|retry| retry.failures.saturating_add(1))
            .unwrap_or(1)
            .min(16);
        let shift = u32::from(failures.saturating_sub(1)).min(6);
        let multiplier = 1_u32 << shift;
        let delay = REGISTRATION_RETRY_MIN
            .saturating_mul(multiplier)
            .min(REGISTRATION_RETRY_MAX);
        self.entries.insert(
            (database_oid, kind),
            RegistrationRetry {
                failures,
                next_attempt_at: now + delay,
            },
        );
        delay
    }

    /// Drops idle backoff state when work for this kind is no longer due.
    pub fn clear_if_idle(&mut self, database_oid: u32, kind: DynamicWorkerKind) {
        self.entries.remove(&(database_oid, kind));
    }

    /// Earliest remaining backoff wait across all entries.
    #[must_use]
    pub fn next_wait(&self, now: Instant) -> Option<Duration> {
        self.entries
            .values()
            .map(|retry| retry.next_attempt_at.saturating_duration_since(now))
            .min()
    }
}

/// Computes the next supervisor latch wait from backoff, safety, lifecycle, and
/// published deadlines.
#[must_use]
pub fn next_wait_duration(
    has_slots: bool,
    last_safety: Instant,
    registration_backoff: &RegistrationBackoff,
    lifecycle_reconcile_at: Option<Instant>,
    snapshots: &[DatabaseWorkSnapshot],
    now: Instant,
    now_ms: i64,
) -> Option<Duration> {
    let mut wait = registration_backoff.next_wait(now);
    if has_slots {
        wait = min_optional_duration(
            wait,
            Some(SAFETY_RECONCILE_INTERVAL.saturating_sub(last_safety.elapsed())),
        );
    }
    if let Some(deadline) = lifecycle_reconcile_at {
        wait = min_optional_duration(wait, Some(deadline.saturating_duration_since(now)));
    }

    for snapshot in snapshots {
        wait = min_optional_duration(wait, deadline_delay(snapshot.next_flush_due_at_ms, now_ms));
        wait = min_optional_duration(
            wait,
            deadline_delay(snapshot.next_maintenance_due_at_ms, now_ms),
        );
    }

    wait.map(|duration| duration.max(Duration::from_millis(1)))
}

#[must_use]
pub fn deadline_delay(deadline_ms: i64, now_ms: i64) -> Option<Duration> {
    if deadline_ms <= 0 {
        return None;
    }
    let delay_ms = deadline_ms.saturating_sub(now_ms).max(1);
    Some(Duration::from_millis(
        u64::try_from(delay_ms).unwrap_or(u64::MAX),
    ))
}

#[must_use]
pub fn min_optional_duration(left: Option<Duration>, right: Option<Duration>) -> Option<Duration> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_backoff_grows_then_caps() {
        let mut backoff = RegistrationBackoff::default();
        let start = Instant::now();
        let first = backoff.failed(1, DynamicWorkerKind::Wal, start);
        assert_eq!(first, REGISTRATION_RETRY_MIN);
        let mut now = start;
        let mut last = first;
        for _ in 0..8 {
            now += last;
            last = backoff.failed(1, DynamicWorkerKind::Wal, now);
        }
        assert_eq!(last, REGISTRATION_RETRY_MAX);
    }

    #[test]
    fn deadline_delay_ignores_unset_and_clamps_past() {
        assert!(deadline_delay(0, 100).is_none());
        assert_eq!(deadline_delay(50, 100), Some(Duration::from_millis(1)));
        assert_eq!(deadline_delay(150, 100), Some(Duration::from_millis(50)));
    }
}
