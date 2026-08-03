//! Coalescing commit-wakeup state for database-scoped workers.

use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

/// Backend-local dirty state that follows PostgreSQL subtransaction outcomes.
///
/// Only the earliest nesting level containing managed DML is needed. Committing
/// a savepoint promotes that state to its parent; aborting that level clears it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionDirty {
    earliest_level: u32,
}

impl TransactionDirty {
    /// Marks managed DML at `nesting_level`.
    pub const fn mark(&mut self, nesting_level: u32) {
        let level = if nesting_level == 0 { 1 } else { nesting_level };
        if self.earliest_level == 0 || level < self.earliest_level {
            self.earliest_level = level;
        }
    }

    /// Promotes dirty work committed by a subtransaction into its parent.
    pub const fn commit_subtransaction(&mut self, nesting_level: u32) {
        if self.earliest_level >= nesting_level && nesting_level > 1 {
            self.earliest_level = nesting_level - 1;
        }
    }

    /// Discards dirty work whose owning subtransaction aborted.
    pub const fn abort_subtransaction(&mut self, nesting_level: u32) {
        if self.earliest_level >= nesting_level {
            self.earliest_level = 0;
        }
    }

    /// Clears and returns whether the top-level transaction contained committed DML.
    pub const fn take(&mut self) -> bool {
        let dirty = self.earliest_level != 0;
        self.earliest_level = 0;
        dirty
    }

    /// Clears all transaction-local state.
    pub const fn clear(&mut self) {
        self.earliest_level = 0;
    }
}

/// Bounded backoff for a commit wake whose WAL is not decodeable yet.
///
/// This covers asynchronous PostgreSQL commits without restoring a permanent
/// polling loop. Callers should keep `confirmed_flush` unchanged across these
/// retries so unrelated WAL cannot advance the slot before the watchdog. Once
/// the bounded window expires, the caller can acknowledge a false-positive wake
/// and rely on the low-frequency correctness watchdog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyWakeRetry {
    initial: Duration,
    maximum: Duration,
    window: Duration,
    started_at: Option<Duration>,
    next: Duration,
}

impl EmptyWakeRetry {
    /// Creates an empty-wake retry policy using monotonic elapsed timestamps.
    #[must_use]
    pub const fn new(initial: Duration, maximum: Duration, window: Duration) -> Self {
        Self {
            initial,
            maximum,
            window,
            started_at: None,
            next: initial,
        }
    }

    /// Returns the next delay, or `None` after the bounded retry window.
    pub fn after_empty(&mut self, now: Duration) -> Option<Duration> {
        let started_at = *self.started_at.get_or_insert(now);
        let elapsed = now.saturating_sub(started_at);
        let remaining = self.window.saturating_sub(elapsed);
        if remaining.is_zero() {
            return None;
        }

        let delay = self.next.min(remaining);
        self.next = self.next.saturating_mul(2).min(self.maximum);
        Some(delay)
    }

    /// Clears retry history after useful work or a new notification cycle.
    pub const fn reset(&mut self) {
        self.started_at = None;
        self.next = self.initial;
    }
}

/// Monotonic, wrapping generation published after managed-table commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakeGeneration(u64);

impl WakeGeneration {
    /// Wraps a raw shared-memory generation value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw generation stored in shared memory.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Worker-local cursor used to collapse any number of commits into one drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakeCursor {
    observed: WakeGeneration,
}

impl WakeCursor {
    /// Starts a cursor at the generation observed during worker registration.
    #[must_use]
    pub const fn new(observed: WakeGeneration) -> Self {
        Self { observed }
    }

    /// Returns true when at least one commit occurred after the last observation.
    #[must_use]
    pub const fn is_pending(self, current: WakeGeneration) -> bool {
        self.observed.0 != current.0
    }

    /// Marks all commits through `current` as observed.
    pub const fn observe(&mut self, current: WakeGeneration) {
        self.observed = current;
    }
}

/// PostgreSQL worker PID stored as a wake target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerPid(i32);

impl WorkerPid {
    /// Wraps a PostgreSQL process ID.
    #[must_use]
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    /// Returns the raw process ID.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Result of publishing a managed commit into the wake registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishWake {
    /// Generation containing the newly published commit.
    pub generation: WakeGeneration,
    /// Current database worker, when it has registered its PID.
    pub worker_pid: Option<WorkerPid>,
}

#[derive(Debug)]
struct AtomicWakeEntry {
    database_oid: AtomicU32,
    generation: AtomicU64,
    worker_pid: AtomicI32,
}

impl AtomicWakeEntry {
    const fn empty() -> Self {
        Self {
            database_oid: AtomicU32::new(0),
            generation: AtomicU64::new(0),
            worker_pid: AtomicI32::new(0),
        }
    }
}

/// Lock-free production registry for commit generations and worker PIDs.
///
/// Slot assignment is a one-time compare/exchange. Established databases then
/// publish with one generation increment and one PID load, so unrelated
/// databases never contend on a global lock.
#[derive(Debug)]
pub struct AtomicWakeRegistry<const N: usize> {
    entries: [AtomicWakeEntry; N],
}

impl<const N: usize> Default for AtomicWakeRegistry<N> {
    fn default() -> Self {
        Self {
            entries: [const { AtomicWakeEntry::empty() }; N],
        }
    }
}

impl<const N: usize> AtomicWakeRegistry<N> {
    /// Publishes one committed transaction and returns the current wake target.
    pub fn publish(&self, database_oid: u32) -> Option<PublishWake> {
        let entry = self.entry(database_oid)?;
        let generation = entry
            .generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        let worker_pid = entry.worker_pid.load(Ordering::Acquire);
        Some(PublishWake {
            generation: WakeGeneration::new(generation),
            worker_pid: (worker_pid > 0).then(|| WorkerPid::new(worker_pid)),
        })
    }

    /// Registers or replaces the worker PID without discarding pending commits.
    pub fn register_worker(
        &self,
        database_oid: u32,
        worker_pid: WorkerPid,
    ) -> Option<WakeGeneration> {
        let entry = self.entry(database_oid)?;
        entry.worker_pid.store(worker_pid.get(), Ordering::Release);
        Some(WakeGeneration::new(
            entry.generation.load(Ordering::Acquire),
        ))
    }

    /// Clears the PID only when it still belongs to the exiting worker.
    pub fn unregister_worker(&self, database_oid: u32, worker_pid: WorkerPid) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        let _ = entry.worker_pid.compare_exchange(
            worker_pid.get(),
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Returns the current database generation without allocating a new entry.
    #[must_use]
    pub fn generation(&self, database_oid: u32) -> Option<WakeGeneration> {
        self.find(database_oid)
            .map(|entry| WakeGeneration::new(entry.generation.load(Ordering::Acquire)))
    }

    fn find(&self, database_oid: u32) -> Option<&AtomicWakeEntry> {
        self.entries
            .iter()
            .find(|entry| entry.database_oid.load(Ordering::Acquire) == database_oid)
    }

    fn entry(&self, database_oid: u32) -> Option<&AtomicWakeEntry> {
        if let Some(entry) = self.find(database_oid) {
            return Some(entry);
        }
        for entry in &self.entries {
            match entry.database_oid.compare_exchange(
                0,
                database_oid,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(entry),
                Err(current) if current == database_oid => return Some(entry),
                Err(_) => {}
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        AtomicWakeRegistry, EmptyWakeRetry, TransactionDirty, WakeCursor, WakeGeneration, WorkerPid,
    };

    #[test]
    fn aborted_savepoint_does_not_publish_a_false_dirty_commit() {
        let mut dirty = TransactionDirty::default();
        dirty.mark(2);
        dirty.abort_subtransaction(2);

        assert!(!dirty.take());
    }

    #[test]
    fn committed_savepoint_promotes_dirty_state_to_the_parent() {
        let mut dirty = TransactionDirty::default();
        dirty.mark(3);
        dirty.commit_subtransaction(3);
        dirty.commit_subtransaction(2);

        assert!(dirty.take());
        assert!(!dirty.take());
    }

    #[test]
    fn inner_abort_preserves_managed_dml_from_an_outer_level() {
        let mut dirty = TransactionDirty::default();
        dirty.mark(1);
        dirty.mark(2);
        dirty.abort_subtransaction(2);

        assert!(dirty.take());
    }

    #[test]
    fn empty_wake_retry_backs_off_and_stops_at_its_window() {
        let mut retry = EmptyWakeRetry::new(
            Duration::from_millis(10),
            Duration::from_millis(200),
            Duration::from_secs(1),
        );

        assert_eq!(
            retry.after_empty(Duration::ZERO),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            retry.after_empty(Duration::from_millis(10)),
            Some(Duration::from_millis(20))
        );
        assert_eq!(
            retry.after_empty(Duration::from_millis(30)),
            Some(Duration::from_millis(40))
        );
        assert_eq!(
            retry.after_empty(Duration::from_millis(999)),
            Some(Duration::from_millis(1))
        );
        assert_eq!(retry.after_empty(Duration::from_secs(1)), None);
    }

    #[test]
    fn empty_wake_retry_reset_starts_a_fresh_window() {
        let mut retry = EmptyWakeRetry::new(
            Duration::from_millis(10),
            Duration::from_millis(200),
            Duration::from_secs(1),
        );

        let _ = retry.after_empty(Duration::ZERO);
        let _ = retry.after_empty(Duration::from_millis(10));
        retry.reset();

        assert_eq!(
            retry.after_empty(Duration::from_secs(5)),
            Some(Duration::from_millis(10))
        );
    }

    #[test]
    fn cursor_coalesces_many_commits_into_one_pending_wake() {
        let mut cursor = WakeCursor::new(WakeGeneration::new(7));

        assert!(!cursor.is_pending(WakeGeneration::new(7)));
        assert!(cursor.is_pending(WakeGeneration::new(1_007)));

        cursor.observe(WakeGeneration::new(1_007));
        assert!(!cursor.is_pending(WakeGeneration::new(1_007)));
    }

    #[test]
    fn wrapping_generation_is_still_detected_as_new_work() {
        let cursor = WakeCursor::new(WakeGeneration::new(u64::MAX));

        assert!(cursor.is_pending(WakeGeneration::new(0)));
    }

    #[test]
    fn registry_preserves_commits_published_before_worker_registration() {
        let registry = AtomicWakeRegistry::<2>::default();

        let published = registry.publish(42).expect("database slot");
        assert_eq!(published.generation, WakeGeneration::new(1));
        assert_eq!(published.worker_pid, None);

        let generation = registry
            .register_worker(42, WorkerPid::new(9001))
            .expect("database slot");
        assert_eq!(generation, WakeGeneration::new(1));
    }

    #[test]
    fn registry_coalesces_generation_and_returns_one_worker_target() {
        let registry = AtomicWakeRegistry::<2>::default();
        registry
            .register_worker(42, WorkerPid::new(9001))
            .expect("database slot");

        for expected in 1..=1_000 {
            let published = registry.publish(42).expect("database slot");
            assert_eq!(published.generation, WakeGeneration::new(expected));
            assert_eq!(published.worker_pid, Some(WorkerPid::new(9001)));
        }
    }

    #[test]
    fn stale_worker_cannot_unregister_replacement() {
        let registry = AtomicWakeRegistry::<1>::default();
        registry
            .register_worker(42, WorkerPid::new(9001))
            .expect("database slot");
        registry
            .register_worker(42, WorkerPid::new(9002))
            .expect("database slot");

        registry.unregister_worker(42, WorkerPid::new(9001));

        assert_eq!(
            registry.publish(42).expect("database slot").worker_pid,
            Some(WorkerPid::new(9002))
        );
    }

    #[test]
    fn full_registry_fails_closed_to_watchdog_recovery() {
        let registry = AtomicWakeRegistry::<1>::default();
        registry.publish(42).expect("first database slot");

        assert_eq!(registry.publish(84), None);
        assert_eq!(registry.register_worker(84, WorkerPid::new(9002)), None);
    }

    #[test]
    fn atomic_registry_publishes_concurrently_without_a_global_lock() {
        let registry = std::sync::Arc::new(AtomicWakeRegistry::<2>::default());
        registry
            .register_worker(42, WorkerPid::new(9001))
            .expect("database slot");
        let mut publishers = Vec::new();
        for _ in 0..8 {
            let registry = std::sync::Arc::clone(&registry);
            publishers.push(std::thread::spawn(move || {
                for _ in 0..1_000 {
                    registry.publish(42).expect("database slot");
                }
            }));
        }
        for publisher in publishers {
            publisher.join().expect("publisher thread");
        }

        assert_eq!(registry.generation(42), Some(WakeGeneration::new(8_000)));
    }
}
