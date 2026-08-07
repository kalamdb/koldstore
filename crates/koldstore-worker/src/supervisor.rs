//! PostgreSQL-free shared state for event-driven KoldStore worker supervision.
//!
//! Wakeups are hints, never the durable source of truth. Logical slots,
//! async_mirror_state, and koldstore.jobs survive process/postmaster failure.
//! Monotonic generations make event coalescing race-safe across worker startup,
//! execution, and exit windows.

use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering};

/// One entry is reserved per database that has published KoldStore work.
pub const SUPERVISOR_REGISTRY_CAPACITY: usize = 256;

pub const EVENT_WAL_DIRTY: u32 = 1 << 0;
pub const EVENT_FLUSH_QUEUE_DIRTY: u32 = 1 << 1;
pub const EVENT_RECOVERY_REQUIRED: u32 = 1 << 2;
pub const EVENT_SCHEDULE_DIRTY: u32 = 1 << 3;

const WORKER_FREE: i32 = 0;
const WORKER_STARTING: i32 = -1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisorPid(i32);

impl SupervisorPid {
    #[must_use]
    pub const fn new(pid: i32) -> Self {
        Self(pid)
    }

    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseWorkSnapshot {
    pub database_oid: u32,
    pub wal_generation: u64,
    pub wal_processed_generation: u64,
    pub maintenance_generation: u64,
    pub maintenance_processed_generation: u64,
    pub flush_generation: u64,
    pub flush_processed_generation: u64,
    pub event_flags: u32,
    /// 0 = no worker, -1 = registration/start in progress, >0 = worker PID.
    pub maintenance_pid: i32,
    pub flush_starting: u32,
    pub flush_running: u32,
    pub flush_limit: u32,
    /// Earliest future pending flush `available_at` as Unix epoch milliseconds.
    /// Zero means no known future queue deadline.
    pub next_flush_due_at_ms: i64,
}

impl DatabaseWorkSnapshot {
    #[must_use]
    pub const fn maintenance_due(self) -> bool {
        self.wal_generation != self.wal_processed_generation
            || self.maintenance_generation != self.maintenance_processed_generation
    }

    #[must_use]
    pub const fn flush_due(self) -> bool {
        self.flush_generation != self.flush_processed_generation
    }

    #[must_use]
    pub const fn flush_workers(self) -> u32 {
        self.flush_starting.saturating_add(self.flush_running)
    }
}

#[derive(Debug)]
struct DatabaseWorkEntry {
    database_oid: AtomicU32,
    wal_generation: AtomicU64,
    wal_processed_generation: AtomicU64,
    maintenance_generation: AtomicU64,
    maintenance_processed_generation: AtomicU64,
    flush_generation: AtomicU64,
    flush_processed_generation: AtomicU64,
    event_flags: AtomicU32,
    maintenance_pid: AtomicI32,
    flush_starting: AtomicU32,
    flush_running: AtomicU32,
    /// Effective per-database cap last reported by a worker. Unknown starts at 1.
    flush_limit: AtomicU32,
    next_flush_due_at_ms: AtomicI64,
}

impl DatabaseWorkEntry {
    const fn empty() -> Self {
        Self {
            database_oid: AtomicU32::new(0),
            wal_generation: AtomicU64::new(0),
            wal_processed_generation: AtomicU64::new(0),
            maintenance_generation: AtomicU64::new(0),
            maintenance_processed_generation: AtomicU64::new(0),
            flush_generation: AtomicU64::new(0),
            flush_processed_generation: AtomicU64::new(0),
            event_flags: AtomicU32::new(0),
            maintenance_pid: AtomicI32::new(WORKER_FREE),
            flush_starting: AtomicU32::new(0),
            flush_running: AtomicU32::new(0),
            flush_limit: AtomicU32::new(1),
            next_flush_due_at_ms: AtomicI64::new(0),
        }
    }

    fn snapshot(&self) -> DatabaseWorkSnapshot {
        DatabaseWorkSnapshot {
            database_oid: self.database_oid.load(Ordering::Acquire),
            wal_generation: self.wal_generation.load(Ordering::Acquire),
            wal_processed_generation: self.wal_processed_generation.load(Ordering::Acquire),
            maintenance_generation: self.maintenance_generation.load(Ordering::Acquire),
            maintenance_processed_generation: self
                .maintenance_processed_generation
                .load(Ordering::Acquire),
            flush_generation: self.flush_generation.load(Ordering::Acquire),
            flush_processed_generation: self.flush_processed_generation.load(Ordering::Acquire),
            event_flags: self.event_flags.load(Ordering::Acquire),
            maintenance_pid: self.maintenance_pid.load(Ordering::Acquire),
            flush_starting: self.flush_starting.load(Ordering::Acquire),
            flush_running: self.flush_running.load(Ordering::Acquire),
            flush_limit: self.flush_limit.load(Ordering::Acquire).max(1),
            next_flush_due_at_ms: self.next_flush_due_at_ms.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug)]
pub struct SupervisorRegistry<const N: usize> {
    supervisor_pid: AtomicI32,
    overflow_reconcile_required: AtomicU32,
    entries: [DatabaseWorkEntry; N],
}

impl<const N: usize> Default for SupervisorRegistry<N> {
    fn default() -> Self {
        Self {
            supervisor_pid: AtomicI32::new(0),
            overflow_reconcile_required: AtomicU32::new(0),
            entries: [const { DatabaseWorkEntry::empty() }; N],
        }
    }
}

impl<const N: usize> SupervisorRegistry<N> {
    pub fn register_supervisor(&self, pid: SupervisorPid) {
        self.supervisor_pid.store(pid.get(), Ordering::Release);
    }

    pub fn unregister_supervisor(&self, pid: SupervisorPid) {
        let _ = self.supervisor_pid.compare_exchange(
            pid.get(),
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    #[must_use]
    pub fn supervisor_pid(&self) -> Option<SupervisorPid> {
        let pid = self.supervisor_pid.load(Ordering::Acquire);
        (pid > 0).then(|| SupervisorPid::new(pid))
    }

    pub fn publish_wal(&self, database_oid: u32) -> Option<SupervisorPid> {
        let entry = self.entry_or_overflow(database_oid)?;
        entry.wal_generation.fetch_add(1, Ordering::AcqRel);
        entry.event_flags.fetch_or(EVENT_WAL_DIRTY, Ordering::AcqRel);
        self.supervisor_pid()
    }

    pub fn publish_flush(&self, database_oid: u32) -> Option<SupervisorPid> {
        let entry = self.entry_or_overflow(database_oid)?;
        entry.flush_generation.fetch_add(1, Ordering::AcqRel);
        entry
            .event_flags
            .fetch_or(EVENT_FLUSH_QUEUE_DIRTY, Ordering::AcqRel);
        self.supervisor_pid()
    }

    pub fn request_recovery(&self, database_oid: u32) -> Option<SupervisorPid> {
        let entry = self.entry_or_overflow(database_oid)?;
        entry.maintenance_generation.fetch_add(1, Ordering::AcqRel);
        entry
            .event_flags
            .fetch_or(EVENT_RECOVERY_REQUIRED, Ordering::AcqRel);
        self.supervisor_pid()
    }

    pub fn publish_schedule(&self, database_oid: u32) -> Option<SupervisorPid> {
        let entry = self.entry_or_overflow(database_oid)?;
        entry.maintenance_generation.fetch_add(1, Ordering::AcqRel);
        entry
            .event_flags
            .fetch_or(EVENT_SCHEDULE_DIRTY, Ordering::AcqRel);
        self.supervisor_pid()
    }

    #[must_use]
    pub fn snapshot(&self, database_oid: u32) -> Option<DatabaseWorkSnapshot> {
        self.find(database_oid).map(DatabaseWorkEntry::snapshot)
    }

    #[must_use]
    pub fn snapshots(&self) -> Vec<DatabaseWorkSnapshot> {
        self.entries
            .iter()
            .filter(|entry| entry.database_oid.load(Ordering::Acquire) != 0)
            .map(DatabaseWorkEntry::snapshot)
            .collect()
    }

    pub fn try_reserve_maintenance(&self, database_oid: u32) -> bool {
        let Some(entry) = self.entry_or_overflow(database_oid) else {
            return false;
        };
        entry
            .maintenance_pid
            .compare_exchange(
                WORKER_FREE,
                WORKER_STARTING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn maintenance_started(&self, database_oid: u32, pid: i32) -> bool {
        let Some(entry) = self.find(database_oid) else {
            return false;
        };
        entry
            .maintenance_pid
            .compare_exchange(
                WORKER_STARTING,
                pid,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn cancel_maintenance_start(&self, database_oid: u32) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        let _ = entry.maintenance_pid.compare_exchange(
            WORKER_STARTING,
            WORKER_FREE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn maintenance_stopped(&self, database_oid: u32, pid: i32) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        let _ = entry.maintenance_pid.compare_exchange(
            pid,
            WORKER_FREE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Clears stale Starting/Running maintenance ownership after an authoritative
    /// PostgreSQL liveness check. Generations remain dirty, so work is retried.
    pub fn clear_stale_maintenance(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            entry.maintenance_pid.store(WORKER_FREE, Ordering::Release);
        }
    }

    pub fn mark_wal_processed(&self, database_oid: u32, generation: u64) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        atomic_max(&entry.wal_processed_generation, generation);
        clear_flag_if_generation_current(
            &entry.wal_generation,
            generation,
            &entry.event_flags,
            EVENT_WAL_DIRTY,
        );
    }

    /// Marks recovery/schedule work through `generation` reconciled without
    /// clearing a concurrent/newer request.
    pub fn mark_maintenance_reconciled(&self, database_oid: u32, generation: u64) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        atomic_max(&entry.maintenance_processed_generation, generation);
        if entry.maintenance_generation.load(Ordering::Acquire) == generation {
            entry.event_flags.fetch_and(
                !(EVENT_RECOVERY_REQUIRED | EVENT_SCHEDULE_DIRTY),
                Ordering::AcqRel,
            );
            if entry.maintenance_generation.load(Ordering::Acquire) != generation {
                entry
                    .event_flags
                    .fetch_or(EVENT_RECOVERY_REQUIRED, Ordering::AcqRel);
            }
        }
    }

    pub fn set_flush_limit(&self, database_oid: u32, limit: u32) {
        let Some(entry) = self.entry_or_overflow(database_oid) else {
            return;
        };
        entry
            .flush_limit
            .store(limit.max(1), Ordering::Release);
    }

    pub fn try_reserve_flush(&self, database_oid: u32, cluster_limit: u32) -> bool {
        let Some(entry) = self.entry_or_overflow(database_oid) else {
            return false;
        };
        let per_db_limit = entry.flush_limit.load(Ordering::Acquire).max(1);
        let db_total = entry
            .flush_starting
            .load(Ordering::Acquire)
            .saturating_add(entry.flush_running.load(Ordering::Acquire));
        if db_total >= per_db_limit {
            return false;
        }
        let cluster_total = self
            .entries
            .iter()
            .map(|item| {
                item.flush_starting
                    .load(Ordering::Acquire)
                    .saturating_add(item.flush_running.load(Ordering::Acquire))
            })
            .fold(0_u32, u32::saturating_add);
        if cluster_total >= cluster_limit.max(1) {
            return false;
        }
        entry.flush_starting.fetch_add(1, Ordering::AcqRel);
        true
    }

    pub fn flush_started(&self, database_oid: u32, effective_limit: u32) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        decrement_if_positive(&entry.flush_starting);
        entry.flush_running.fetch_add(1, Ordering::AcqRel);
        entry
            .flush_limit
            .store(effective_limit.max(1), Ordering::Release);
    }

    pub fn cancel_flush_start(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            decrement_if_positive(&entry.flush_starting);
        }
    }

    pub fn flush_stopped(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            decrement_if_positive(&entry.flush_running);
        }
    }

    /// Authoritatively resets Starting/Running counts during rare lifecycle
    /// reconciliation. Normal dispatch never queries pg_stat_activity.
    pub fn reconcile_flush_counts(&self, database_oid: u32, running: u32) {
        let Some(entry) = self.entry_or_overflow(database_oid) else {
            return;
        };
        entry.flush_starting.store(0, Ordering::Release);
        entry.flush_running.store(running, Ordering::Release);
    }

    pub fn mark_flush_processed(&self, database_oid: u32, generation: u64) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        atomic_max(&entry.flush_processed_generation, generation);
        clear_flag_if_generation_current(
            &entry.flush_generation,
            generation,
            &entry.event_flags,
            EVENT_FLUSH_QUEUE_DIRTY,
        );
    }

    /// Records the earliest known future `available_at` for pending flush work.
    pub fn schedule_flush_at_ms(&self, database_oid: u32, deadline_ms: i64) {
        if deadline_ms <= 0 {
            return;
        }
        let Some(entry) = self.entry_or_overflow(database_oid) else {
            return;
        };
        atomic_min_nonzero(&entry.next_flush_due_at_ms, deadline_ms);
    }

    /// Clears the current queue deadline after an authoritative empty/no-future
    /// probe. A concurrent enqueue still advances `flush_generation` and cannot
    /// be lost even if it races this store.
    pub fn clear_flush_deadline(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            entry.next_flush_due_at_ms.store(0, Ordering::Release);
        }
    }

    /// Clears a due deadline only if it still matches the sampled value.
    pub fn consume_flush_deadline(&self, database_oid: u32, sampled_ms: i64) -> bool {
        let Some(entry) = self.find(database_oid) else {
            return false;
        };
        sampled_ms > 0
            && entry
                .next_flush_due_at_ms
                .compare_exchange(sampled_ms, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    #[must_use]
    pub fn overflow_reconcile_required(&self) -> bool {
        self.overflow_reconcile_required.load(Ordering::Acquire) != 0
    }

    pub fn clear_overflow_reconcile_required(&self) {
        self.overflow_reconcile_required.store(0, Ordering::Release);
    }

    fn find(&self, database_oid: u32) -> Option<&DatabaseWorkEntry> {
        self.entries
            .iter()
            .find(|entry| entry.database_oid.load(Ordering::Acquire) == database_oid)
    }

    fn entry_or_overflow(&self, database_oid: u32) -> Option<&DatabaseWorkEntry> {
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
        self.overflow_reconcile_required.store(1, Ordering::Release);
        None
    }
}

fn clear_flag_if_generation_current(
    current_generation: &AtomicU64,
    processed_generation: u64,
    flags: &AtomicU32,
    flag: u32,
) {
    if current_generation.load(Ordering::Acquire) != processed_generation {
        return;
    }
    flags.fetch_and(!flag, Ordering::AcqRel);
    if current_generation.load(Ordering::Acquire) != processed_generation {
        flags.fetch_or(flag, Ordering::AcqRel);
    }
}

fn atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Acquire);
    while current < value {
        match target.compare_exchange_weak(current, value, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn atomic_min_nonzero(target: &AtomicI64, value: i64) {
    let mut current = target.load(Ordering::Acquire);
    loop {
        if current > 0 && current <= value {
            return;
        }
        match target.compare_exchange_weak(current, value, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn decrement_if_positive(target: &AtomicU32) {
    let mut current = target.load(Ordering::Acquire);
    while current > 0 {
        match target.compare_exchange_weak(
            current,
            current - 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SupervisorPid, SupervisorRegistry, EVENT_FLUSH_QUEUE_DIRTY, EVENT_RECOVERY_REQUIRED,
        EVENT_WAL_DIRTY,
    };

    #[test]
    fn committed_events_coalesce_without_losing_generations() {
        let registry = SupervisorRegistry::<2>::default();
        registry.register_supervisor(SupervisorPid::new(500));
        for _ in 0..100 {
            assert_eq!(registry.publish_wal(42), Some(SupervisorPid::new(500)));
        }
        let snapshot = registry.snapshot(42).unwrap();
        assert_eq!(snapshot.wal_generation, 100);
        assert_eq!(snapshot.event_flags & EVENT_WAL_DIRTY, EVENT_WAL_DIRTY);
        registry.mark_wal_processed(42, 100);
        assert!(!registry.snapshot(42).unwrap().maintenance_due());
    }

    #[test]
    fn maintenance_generation_cannot_erase_newer_recovery() {
        let registry = SupervisorRegistry::<1>::default();
        registry.request_recovery(42);
        let first = registry.snapshot(42).unwrap().maintenance_generation;
        registry.publish_schedule(42);
        registry.mark_maintenance_reconciled(42, first);
        let snapshot = registry.snapshot(42).unwrap();
        assert_eq!(snapshot.maintenance_generation, 2);
        assert!(snapshot.maintenance_due());
    }

    #[test]
    fn worker_reservations_include_starting_workers() {
        let registry = SupervisorRegistry::<2>::default();
        registry.set_flush_limit(42, 2);
        assert!(registry.try_reserve_flush(42, 4));
        assert!(registry.try_reserve_flush(42, 4));
        assert!(!registry.try_reserve_flush(42, 4));
        assert_eq!(registry.snapshot(42).unwrap().flush_workers(), 2);
        registry.flush_started(42, 2);
        assert_eq!(registry.snapshot(42).unwrap().flush_workers(), 2);
        registry.flush_stopped(42);
        assert_eq!(registry.snapshot(42).unwrap().flush_workers(), 1);
    }

    #[test]
    fn maintenance_reservation_is_single_owner() {
        let registry = SupervisorRegistry::<1>::default();
        registry.request_recovery(42);
        assert!(registry.try_reserve_maintenance(42));
        assert!(!registry.try_reserve_maintenance(42));
        assert!(registry.maintenance_started(42, 9001));
        registry.maintenance_stopped(42, 9001);
        assert!(registry.try_reserve_maintenance(42));
    }

    #[test]
    fn queue_generation_clear_cannot_erase_newer_work() {
        let registry = SupervisorRegistry::<1>::default();
        registry.publish_flush(42);
        let first = registry.snapshot(42).unwrap().flush_generation;
        registry.publish_flush(42);
        registry.mark_flush_processed(42, first);
        let snapshot = registry.snapshot(42).unwrap();
        assert_eq!(snapshot.flush_generation, 2);
        assert_ne!(snapshot.event_flags & EVENT_FLUSH_QUEUE_DIRTY, 0);
        assert!(snapshot.flush_due());
    }

    #[test]
    fn future_flush_deadline_keeps_earliest_value() {
        let registry = SupervisorRegistry::<1>::default();
        registry.schedule_flush_at_ms(42, 5_000);
        registry.schedule_flush_at_ms(42, 7_000);
        registry.schedule_flush_at_ms(42, 3_000);
        assert_eq!(registry.snapshot(42).unwrap().next_flush_due_at_ms, 3_000);
        assert!(registry.consume_flush_deadline(42, 3_000));
        assert_eq!(registry.snapshot(42).unwrap().next_flush_due_at_ms, 0);
    }

    #[test]
    fn registry_overflow_fails_closed_to_reconciliation() {
        let registry = SupervisorRegistry::<1>::default();
        let _ = registry.publish_wal(42);
        assert_eq!(registry.publish_wal(84), None);
        assert!(registry.overflow_reconcile_required());
    }

    #[test]
    fn recovery_flag_has_own_generation() {
        let registry = SupervisorRegistry::<1>::default();
        registry.request_recovery(42);
        let snapshot = registry.snapshot(42).unwrap();
        assert_eq!(snapshot.wal_generation, 0);
        assert_eq!(snapshot.maintenance_generation, 1);
        assert_eq!(
            snapshot.event_flags & EVENT_RECOVERY_REQUIRED,
            EVENT_RECOVERY_REQUIRED
        );
    }
}
