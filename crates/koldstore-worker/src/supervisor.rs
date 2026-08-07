//! PostgreSQL-free shared state for event-driven KoldStore worker supervision.
//!
//! Wakeups are deliberately treated as hints.  The durable sources of truth
//! remain PostgreSQL logical slots and `koldstore.jobs`; generations here make
//! those hints coalescing and race-safe across worker startup/exit windows.

use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};

/// One entry is reserved per database that has published KoldStore work.
pub const SUPERVISOR_REGISTRY_CAPACITY: usize = 256;

/// Database has committed WAL that may need mirror application/slot advance.
pub const EVENT_WAL_DIRTY: u32 = 1 << 0;
/// Database has a committed flush queue mutation that needs dispatch.
pub const EVENT_FLUSH_QUEUE_DIRTY: u32 = 1 << 1;
/// Database needs a durable startup/crash reconciliation pass.
pub const EVENT_RECOVERY_REQUIRED: u32 = 1 << 2;
/// Database scheduling metadata changed and should be reconciled.
pub const EVENT_SCHEDULE_DIRTY: u32 = 1 << 3;

const WORKER_FREE: i32 = 0;
const WORKER_STARTING: i32 = -1;

/// PID stored in shared supervisor state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisorPid(i32);

impl SupervisorPid {
    /// Wraps a PostgreSQL process id.
    #[must_use]
    pub const fn new(pid: i32) -> Self {
        Self(pid)
    }

    /// Raw process id.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Snapshot used by the PostgreSQL adapter without exposing atomic internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseWorkSnapshot {
    pub database_oid: u32,
    pub wal_generation: u64,
    pub wal_processed_generation: u64,
    pub flush_generation: u64,
    pub flush_processed_generation: u64,
    pub event_flags: u32,
    pub maintenance_pid: i32,
    pub flush_starting: u32,
    pub flush_running: u32,
    pub flush_limit: u32,
}

impl DatabaseWorkSnapshot {
    /// True when a WAL/recovery maintenance worker is still required.
    #[must_use]
    pub const fn maintenance_due(self) -> bool {
        self.wal_generation != self.wal_processed_generation
            || self.event_flags & (EVENT_WAL_DIRTY | EVENT_RECOVERY_REQUIRED | EVENT_SCHEDULE_DIRTY)
                != 0
    }

    /// True when committed queue work has not yet been acknowledged as drained.
    #[must_use]
    pub const fn flush_due(self) -> bool {
        self.flush_generation != self.flush_processed_generation
            || self.event_flags & EVENT_FLUSH_QUEUE_DIRTY != 0
    }

    /// Number of flush workers reserved or running for this database.
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
    flush_generation: AtomicU64,
    flush_processed_generation: AtomicU64,
    event_flags: AtomicU32,
    /// 0 = free, -1 = registration/start in progress, >0 = live worker PID.
    maintenance_pid: AtomicI32,
    flush_starting: AtomicU32,
    flush_running: AtomicU32,
    /// Effective per-database cap last reported by a worker.  Unknown starts at 1.
    flush_limit: AtomicU32,
}

impl DatabaseWorkEntry {
    const fn empty() -> Self {
        Self {
            database_oid: AtomicU32::new(0),
            wal_generation: AtomicU64::new(0),
            wal_processed_generation: AtomicU64::new(0),
            flush_generation: AtomicU64::new(0),
            flush_processed_generation: AtomicU64::new(0),
            event_flags: AtomicU32::new(0),
            maintenance_pid: AtomicI32::new(WORKER_FREE),
            flush_starting: AtomicU32::new(0),
            flush_running: AtomicU32::new(0),
            flush_limit: AtomicU32::new(1),
        }
    }

    fn snapshot(&self) -> DatabaseWorkSnapshot {
        DatabaseWorkSnapshot {
            database_oid: self.database_oid.load(Ordering::Acquire),
            wal_generation: self.wal_generation.load(Ordering::Acquire),
            wal_processed_generation: self.wal_processed_generation.load(Ordering::Acquire),
            flush_generation: self.flush_generation.load(Ordering::Acquire),
            flush_processed_generation: self.flush_processed_generation.load(Ordering::Acquire),
            event_flags: self.event_flags.load(Ordering::Acquire),
            maintenance_pid: self.maintenance_pid.load(Ordering::Acquire),
            flush_starting: self.flush_starting.load(Ordering::Acquire),
            flush_running: self.flush_running.load(Ordering::Acquire),
            flush_limit: self.flush_limit.load(Ordering::Acquire).max(1),
        }
    }
}

/// Fixed shared-memory registry owned by the single cluster supervisor.
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
    /// Registers the current static supervisor PID.
    pub fn register_supervisor(&self, pid: SupervisorPid) {
        self.supervisor_pid.store(pid.get(), Ordering::Release);
    }

    /// Clears the supervisor PID only when it still belongs to `pid`.
    pub fn unregister_supervisor(&self, pid: SupervisorPid) {
        let _ = self.supervisor_pid.compare_exchange(
            pid.get(),
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Current cluster supervisor, if registered.
    #[must_use]
    pub fn supervisor_pid(&self) -> Option<SupervisorPid> {
        let pid = self.supervisor_pid.load(Ordering::Acquire);
        (pid > 0).then(|| SupervisorPid::new(pid))
    }

    /// Publishes one committed WAL hint and returns the current supervisor PID.
    pub fn publish_wal(&self, database_oid: u32) -> Option<SupervisorPid> {
        let entry = self.entry_or_overflow(database_oid)?;
        entry.wal_generation.fetch_add(1, Ordering::AcqRel);
        entry.event_flags.fetch_or(EVENT_WAL_DIRTY, Ordering::AcqRel);
        self.supervisor_pid()
    }

    /// Publishes one committed flush-queue mutation.
    pub fn publish_flush(&self, database_oid: u32) -> Option<SupervisorPid> {
        let entry = self.entry_or_overflow(database_oid)?;
        entry.flush_generation.fetch_add(1, Ordering::AcqRel);
        entry
            .event_flags
            .fetch_or(EVENT_FLUSH_QUEUE_DIRTY, Ordering::AcqRel);
        self.supervisor_pid()
    }

    /// Requests a crash/startup reconciliation for a database.
    pub fn request_recovery(&self, database_oid: u32) -> Option<SupervisorPid> {
        let entry = self.entry_or_overflow(database_oid)?;
        entry
            .event_flags
            .fetch_or(EVENT_RECOVERY_REQUIRED, Ordering::AcqRel);
        self.supervisor_pid()
    }

    /// Requests scheduler metadata reconciliation for a database.
    pub fn publish_schedule(&self, database_oid: u32) -> Option<SupervisorPid> {
        let entry = self.entry_or_overflow(database_oid)?;
        entry
            .event_flags
            .fetch_or(EVENT_SCHEDULE_DIRTY, Ordering::AcqRel);
        self.supervisor_pid()
    }

    /// Reads one database snapshot without allocating a new registry entry.
    #[must_use]
    pub fn snapshot(&self, database_oid: u32) -> Option<DatabaseWorkSnapshot> {
        self.find(database_oid).map(DatabaseWorkEntry::snapshot)
    }

    /// Returns snapshots for all allocated database entries.
    #[must_use]
    pub fn snapshots(&self) -> Vec<DatabaseWorkSnapshot> {
        self.entries
            .iter()
            .filter(|entry| entry.database_oid.load(Ordering::Acquire) != 0)
            .map(DatabaseWorkEntry::snapshot)
            .collect()
    }

    /// Reserves the single maintenance-worker slot for a database.
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

    /// Converts a reserved maintenance worker to a live PID.
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

    /// Releases a failed maintenance-worker registration.
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

    /// Releases a live maintenance worker without clearing newer work.
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

    /// Marks WAL generations through `generation` as drained.
    pub fn mark_wal_processed(&self, database_oid: u32, generation: u64) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        atomic_max(&entry.wal_processed_generation, generation);
        if entry.wal_generation.load(Ordering::Acquire) == generation {
            entry.event_flags.fetch_and(!EVENT_WAL_DIRTY, Ordering::AcqRel);
            // A concurrent publisher can land between the comparison and clear.
            if entry.wal_generation.load(Ordering::Acquire) != generation {
                entry.event_flags.fetch_or(EVENT_WAL_DIRTY, Ordering::AcqRel);
            }
        }
    }

    /// Clears recovery/schedule flags after a successful DB-local reconciliation.
    pub fn mark_maintenance_reconciled(&self, database_oid: u32) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        entry
            .event_flags
            .fetch_and(!(EVENT_RECOVERY_REQUIRED | EVENT_SCHEDULE_DIRTY), Ordering::AcqRel);
    }

    /// Records the effective flush-worker limit learned from a database worker.
    pub fn set_flush_limit(&self, database_oid: u32, limit: u32) {
        let Some(entry) = self.entry_or_overflow(database_oid) else {
            return;
        };
        entry.flush_limit.store(limit.max(1), Ordering::Release);
    }

    /// Reserves one flush-worker slot. Only the supervisor calls this normal path.
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
        let cluster_total: u32 = self
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

    /// Moves one reserved flush worker from starting to running.
    pub fn flush_started(&self, database_oid: u32, effective_limit: u32) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        decrement_if_positive(&entry.flush_starting);
        entry.flush_running.fetch_add(1, Ordering::AcqRel);
        entry.flush_limit.store(effective_limit.max(1), Ordering::Release);
    }

    /// Releases a registration reservation that never started.
    pub fn cancel_flush_start(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            decrement_if_positive(&entry.flush_starting);
        }
    }

    /// Releases one running flush worker.
    pub fn flush_stopped(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            decrement_if_positive(&entry.flush_running);
        }
    }

    /// Marks a queue generation drained if no newer generation has arrived.
    pub fn mark_flush_processed(&self, database_oid: u32, generation: u64) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        atomic_max(&entry.flush_processed_generation, generation);
        if entry.flush_generation.load(Ordering::Acquire) == generation {
            entry
                .event_flags
                .fetch_and(!EVENT_FLUSH_QUEUE_DIRTY, Ordering::AcqRel);
            if entry.flush_generation.load(Ordering::Acquire) != generation {
                entry
                    .event_flags
                    .fetch_or(EVENT_FLUSH_QUEUE_DIRTY, Ordering::AcqRel);
            }
        }
    }

    /// Forces a stale maintenance reservation free during safety reconciliation.
    pub fn clear_stale_maintenance(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            entry.maintenance_pid.store(WORKER_FREE, Ordering::Release);
        }
    }

    /// Reconciles flush counts from a rare authoritative PostgreSQL process scan.
    pub fn reconcile_flush_counts(&self, database_oid: u32, running: u32) {
        if let Some(entry) = self.entry_or_overflow(database_oid) {
            entry.flush_starting.store(0, Ordering::Release);
            entry.flush_running.store(running, Ordering::Release);
        }
    }

    /// True when a publication could not allocate an entry and a conservative
    /// cluster reconciliation is required.
    #[must_use]
    pub fn overflow_reconcile_required(&self) -> bool {
        self.overflow_reconcile_required.load(Ordering::Acquire) != 0
    }

    /// Clears the overflow marker after a successful authoritative scan.
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

fn atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Acquire);
    while current < value {
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
        assert_eq!(registry.snapshot(42).unwrap().event_flags & EVENT_WAL_DIRTY, 0);
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
        assert_ne!(
            snapshot.event_flags & EVENT_FLUSH_QUEUE_DIRTY,
            0,
            "older drain acknowledgement must not clear newer queue work"
        );
    }

    #[test]
    fn registry_overflow_fails_closed_to_reconciliation() {
        let registry = SupervisorRegistry::<1>::default();
        registry.publish_wal(42).unwrap_or_else(|| SupervisorPid::new(0));
        assert_eq!(registry.publish_wal(84), None);
        assert!(registry.overflow_reconcile_required());
    }

    #[test]
    fn recovery_flag_is_independent_of_wal_generation() {
        let registry = SupervisorRegistry::<1>::default();
        registry.request_recovery(42);
        let snapshot = registry.snapshot(42).unwrap();
        assert_eq!(snapshot.wal_generation, 0);
        assert_eq!(
            snapshot.event_flags & EVENT_RECOVERY_REQUIRED,
            EVENT_RECOVERY_REQUIRED
        );
    }
}
