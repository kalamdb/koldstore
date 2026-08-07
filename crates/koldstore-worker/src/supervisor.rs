//! Lock-free shared state for the event-driven KoldStore supervisor.
//!
//! Latches are latency hints only. Durable truth remains in logical slots,
//! async_mirror_state, koldstore.jobs, attempt tokens, and segment ownership.

use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering};

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
    /// 0 = free, -1 = starting, >0 = live worker PID.
    pub maintenance_pid: i32,
    pub flush_starting: u32,
    pub flush_running: u32,
    pub flush_limit: u32,
    /// Earliest future pending flush `available_at`, Unix epoch milliseconds.
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
        entry
            .event_flags
            .fetch_or(EVENT_WAL_DIRTY, Ordering::AcqRel);
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

    /// Copies allocated database entries into a caller-owned reusable buffer.
    /// The permanent supervisor can keep this Vec for its lifetime, avoiding a
    /// fresh allocation on every latch wake/deadline check.
    pub fn snapshots_into(&self, out: &mut Vec<DatabaseWorkSnapshot>) {
        out.clear();
        if out.capacity() < N {
            out.reserve(N - out.capacity());
        }
        for entry in &self.entries {
            if entry.database_oid.load(Ordering::Acquire) != 0 {
                out.push(entry.snapshot());
            }
        }
    }

    #[must_use]
    pub fn snapshots(&self) -> Vec<DatabaseWorkSnapshot> {
        let mut snapshots = Vec::with_capacity(N);
        self.snapshots_into(&mut snapshots);
        snapshots
    }

    /// Returns the current cluster-wide count of starting + running flush workers.
    /// The supervisor samples this once per dispatch pass rather than rescanning
    /// all entries for every individual reservation.
    #[must_use]
    pub fn flush_workers_total(&self) -> u32 {
        self.entries
            .iter()
            .map(|entry| {
                entry
                    .flush_starting
                    .load(Ordering::Acquire)
                    .saturating_add(entry.flush_running.load(Ordering::Acquire))
            })
            .fold(0_u32, u32::saturating_add)
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
            .compare_exchange(WORKER_STARTING, pid, Ordering::AcqRel, Ordering::Acquire)
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
        entry.flush_limit.store(limit.max(1), Ordering::Release);
    }

    /// Reserves one per-database flush slot. Cluster capacity is owned by the
    /// single supervisor and is sampled once per dispatch pass.
    pub fn try_reserve_flush(&self, database_oid: u32) -> bool {
        let Some(entry) = self.entry_or_overflow(database_oid) else {
            return false;
        };
        let per_db_limit = entry.flush_limit.load(Ordering::Acquire).max(1);
        let mut current = entry
            .flush_starting
            .load(Ordering::Acquire)
            .saturating_add(entry.flush_running.load(Ordering::Acquire));
        while current < per_db_limit {
            let starting = entry.flush_starting.load(Ordering::Acquire);
            let running = entry.flush_running.load(Ordering::Acquire);
            current = starting.saturating_add(running);
            if current >= per_db_limit {
                return false;
            }
            match entry.flush_starting.compare_exchange_weak(
                starting,
                starting.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
        false
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

    pub fn schedule_flush_at_ms(&self, database_oid: u32, deadline_ms: i64) {
        if deadline_ms <= 0 {
            return;
        }
        let Some(entry) = self.entry_or_overflow(database_oid) else {
            return;
        };
        atomic_min_nonzero(&entry.next_flush_due_at_ms, deadline_ms);
    }

    pub fn clear_flush_deadline(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            entry.next_flush_due_at_ms.store(0, Ordering::Release);
        }
    }

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

    /// Open-addressed lookup. Entries are never deleted during postmaster life,
    /// so the first empty slot terminates the probe safely. This replaces an
    /// O(N) scan on every managed-transaction commit wake publication.
    fn find(&self, database_oid: u32) -> Option<&DatabaseWorkEntry> {
        if N == 0 || database_oid == 0 {
            return None;
        }
        let start = registry_start_index::<N>(database_oid);
        for offset in 0..N {
            let entry = &self.entries[(start + offset) % N];
            match entry.database_oid.load(Ordering::Acquire) {
                current if current == database_oid => return Some(entry),
                0 => return None,
                _ => {}
            }
        }
        None
    }

    fn entry_or_overflow(&self, database_oid: u32) -> Option<&DatabaseWorkEntry> {
        if N == 0 || database_oid == 0 {
            self.overflow_reconcile_required.store(1, Ordering::Release);
            return None;
        }
        let start = registry_start_index::<N>(database_oid);
        for offset in 0..N {
            let entry = &self.entries[(start + offset) % N];
            match entry.database_oid.load(Ordering::Acquire) {
                current if current == database_oid => return Some(entry),
                0 => match entry.database_oid.compare_exchange(
                    0,
                    database_oid,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Some(entry),
                    Err(current) if current == database_oid => return Some(entry),
                    Err(_) => continue,
                },
                _ => {}
            }
        }
        self.overflow_reconcile_required.store(1, Ordering::Release);
        None
    }
}

fn registry_start_index<const N: usize>(database_oid: u32) -> usize {
    debug_assert!(N > 0);
    (database_oid as usize).wrapping_mul(0x9E37_79B1usize) % N
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
    use super::{SupervisorRegistry, EVENT_WAL_DIRTY};

    #[test]
    fn generations_coalesce_and_clear_safely() {
        let registry = SupervisorRegistry::<1>::default();
        for _ in 0..10 {
            let _ = registry.publish_wal(42);
        }
        let snapshot = registry.snapshot(42).unwrap();
        assert_eq!(snapshot.wal_generation, 10);
        assert_ne!(snapshot.event_flags & EVENT_WAL_DIRTY, 0);
        registry.mark_wal_processed(42, 10);
        assert!(!registry.snapshot(42).unwrap().maintenance_due());
    }

    #[test]
    fn colliding_database_oids_probe_without_losing_entries() {
        let registry = SupervisorRegistry::<4>::default();
        // For a power-of-two capacity these OIDs collide modulo the registry.
        let _ = registry.publish_wal(1);
        let _ = registry.publish_wal(5);
        let _ = registry.publish_wal(9);
        assert_eq!(registry.snapshot(1).unwrap().wal_generation, 1);
        assert_eq!(registry.snapshot(5).unwrap().wal_generation, 1);
        assert_eq!(registry.snapshot(9).unwrap().wal_generation, 1);
        assert_eq!(registry.snapshots().len(), 3);
    }

    #[test]
    fn reusable_snapshot_buffer_does_not_accumulate_stale_entries() {
        let registry = SupervisorRegistry::<4>::default();
        let _ = registry.publish_wal(42);
        let mut snapshots = Vec::new();
        registry.snapshots_into(&mut snapshots);
        assert_eq!(snapshots.len(), 1);
        registry.snapshots_into(&mut snapshots);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].database_oid, 42);
    }

    #[test]
    fn newer_maintenance_generation_survives_old_ack() {
        let registry = SupervisorRegistry::<1>::default();
        let _ = registry.request_recovery(42);
        let first = registry.snapshot(42).unwrap().maintenance_generation;
        let _ = registry.publish_schedule(42);
        registry.mark_maintenance_reconciled(42, first);
        assert!(registry.snapshot(42).unwrap().maintenance_due());
    }

    #[test]
    fn starting_workers_count_toward_flush_capacity() {
        let registry = SupervisorRegistry::<1>::default();
        registry.set_flush_limit(42, 2);
        assert!(registry.try_reserve_flush(42));
        assert!(registry.try_reserve_flush(42));
        assert!(!registry.try_reserve_flush(42));
        assert_eq!(registry.snapshot(42).unwrap().flush_workers(), 2);
        assert_eq!(registry.flush_workers_total(), 2);
    }

    #[test]
    fn future_deadline_keeps_earliest_value() {
        let registry = SupervisorRegistry::<1>::default();
        registry.schedule_flush_at_ms(42, 5_000);
        registry.schedule_flush_at_ms(42, 7_000);
        registry.schedule_flush_at_ms(42, 3_000);
        assert_eq!(registry.snapshot(42).unwrap().next_flush_due_at_ms, 3_000);
        assert!(registry.consume_flush_deadline(42, 3_000));
    }

    #[test]
    fn overflow_requires_reconciliation() {
        let registry = SupervisorRegistry::<1>::default();
        let _ = registry.publish_wal(42);
        assert_eq!(registry.publish_wal(84), None);
        assert!(registry.overflow_reconcile_required());
    }
}
