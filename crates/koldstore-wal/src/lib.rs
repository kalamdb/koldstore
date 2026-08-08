//! PostgreSQL-free lifecycle primitives for the KoldStore WAL service.
//!
//! The WAL service is intentionally distinct from scheduled maintenance:
//! one lightweight applier may stay registered per KoldStore-active database,
//! sleep on a PostgreSQL latch, and wake for committed WAL. PostgreSQL process
//! registration, shared-memory allocation, SPI, and logical decoding remain in
//! `pg_koldstore`; this crate owns only identity and lock-free lifecycle state.
//!
//! The mirror remains the lower-level storage contract used by WAL apply.

use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};

/// Mirror decoding and write-planning contracts beneath the WAL service.
pub use koldstore_mirror as mirror;

/// The mirror's bounded apply batch is also the WAL service's default batch.
pub const WAL_APPLY_BATCH_ROWS: usize = mirror::APPLY_BATCH_ROWS;
/// Maximum database entries tracked by the shared WAL-applier registry.
pub const WAL_APPLIER_REGISTRY_CAPACITY: usize = 256;

const WORKER_FREE: i32 = 0;
const WORKER_STARTING: i32 = -1;

/// Stable PostgreSQL backend type for one database WAL applier.
#[must_use]
pub fn wal_applier_worker_type(database_oid: u32) -> String {
    format!("koldstore wal applier {database_oid}")
}

/// Lock-free view of one database WAL-applier lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalApplierSnapshot {
    pub database_oid: u32,
    /// Whether the database still owns KoldStore async-capture infrastructure.
    pub required: bool,
    /// `0 = stopped`, `-1 = starting`, `>0 = live PostgreSQL PID`.
    pub pid: i32,
}

impl WalApplierSnapshot {
    #[must_use]
    pub const fn running(self) -> bool {
        self.pid > 0
    }

    #[must_use]
    pub const fn starting(self) -> bool {
        self.pid < 0
    }
}

#[derive(Debug)]
struct WalApplierEntry {
    database_oid: AtomicU32,
    required: AtomicU32,
    pid: AtomicI32,
}

impl WalApplierEntry {
    const fn empty() -> Self {
        Self {
            database_oid: AtomicU32::new(0),
            required: AtomicU32::new(0),
            pid: AtomicI32::new(WORKER_FREE),
        }
    }

    fn snapshot(&self) -> WalApplierSnapshot {
        WalApplierSnapshot {
            database_oid: self.database_oid.load(Ordering::Acquire),
            required: self.required.load(Ordering::Acquire) != 0,
            pid: self.pid.load(Ordering::Acquire),
        }
    }
}

/// Fixed-capacity shared registry for persistent database WAL appliers.
///
/// Database identity entries remain allocated during a postmaster lifetime,
/// while [`Self::require`] and [`Self::disable`] toggle whether the supervisor
/// must keep the service resident. This keeps foreground and supervisor
/// operations lock-free and bounded without respawning an applier after its
/// logical slot was deliberately removed.
#[derive(Debug)]
pub struct WalApplierRegistry<const N: usize> {
    overflow_reconcile_required: AtomicU32,
    entries: [WalApplierEntry; N],
}

impl<const N: usize> Default for WalApplierRegistry<N> {
    fn default() -> Self {
        Self {
            overflow_reconcile_required: AtomicU32::new(0),
            entries: [const { WalApplierEntry::empty() }; N],
        }
    }
}

impl<const N: usize> WalApplierRegistry<N> {
    /// Marks the database as requiring a permanently registered WAL service.
    pub fn require(&self, database_oid: u32) -> bool {
        let Some(entry) = self.entry_or_overflow(database_oid) else {
            return false;
        };
        entry.required.store(1, Ordering::Release);
        true
    }

    /// Stops future supervisor restarts after deliberate capture teardown.
    pub fn disable(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            entry.required.store(0, Ordering::Release);
        }
    }

    /// Reserves the one applier slot for `database_oid`.
    pub fn try_reserve(&self, database_oid: u32) -> bool {
        let Some(entry) = self.find(database_oid) else {
            return false;
        };
        if entry.required.load(Ordering::Acquire) == 0 {
            return false;
        }
        entry
            .pid
            .compare_exchange(
                WORKER_FREE,
                WORKER_STARTING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Publishes the PID after a dynamically registered worker connects.
    pub fn started(&self, database_oid: u32, pid: i32) -> bool {
        let Some(entry) = self.find(database_oid) else {
            return false;
        };
        pid > 0
            && entry.required.load(Ordering::Acquire) != 0
            && entry
                .pid
                .compare_exchange(WORKER_STARTING, pid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    /// Releases a STARTING reservation after registration failure.
    pub fn cancel_start(&self, database_oid: u32) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        let _ = entry.pid.compare_exchange(
            WORKER_STARTING,
            WORKER_FREE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Releases a live PID only when it still belongs to the exiting process.
    pub fn stopped(&self, database_oid: u32, pid: i32) {
        let Some(entry) = self.find(database_oid) else {
            return;
        };
        let _ = entry
            .pid
            .compare_exchange(pid, WORKER_FREE, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Repairs a stale STARTING/PID value from PostgreSQL's process list.
    pub fn clear_stale(&self, database_oid: u32) {
        if let Some(entry) = self.find(database_oid) {
            entry.pid.store(WORKER_FREE, Ordering::Release);
        }
    }

    #[must_use]
    pub fn snapshot(&self, database_oid: u32) -> Option<WalApplierSnapshot> {
        self.find(database_oid).map(WalApplierEntry::snapshot)
    }

    /// Copies allocated entries into a reusable caller buffer.
    pub fn snapshots_into(&self, out: &mut Vec<WalApplierSnapshot>) {
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
    pub fn overflow_reconcile_required(&self) -> bool {
        self.overflow_reconcile_required.load(Ordering::Acquire) != 0
    }

    pub fn clear_overflow_reconcile_required(&self) {
        self.overflow_reconcile_required.store(0, Ordering::Release);
    }

    fn find(&self, database_oid: u32) -> Option<&WalApplierEntry> {
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

    fn entry_or_overflow(&self, database_oid: u32) -> Option<&WalApplierEntry> {
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

#[cfg(test)]
mod tests {
    use super::{WalApplierRegistry, WAL_APPLY_BATCH_ROWS};

    #[test]
    fn registry_allows_exactly_one_required_applier_per_database() {
        let registry = WalApplierRegistry::<2>::default();
        assert!(!registry.try_reserve(42));
        assert!(registry.require(42));
        assert!(registry.try_reserve(42));
        assert!(!registry.try_reserve(42));
        assert!(registry.started(42, 1001));
        let snapshot = registry.snapshot(42).unwrap();
        assert!(snapshot.required);
        assert_eq!(snapshot.pid, 1001);
        registry.stopped(42, 1001);
        assert_eq!(registry.snapshot(42).unwrap().pid, 0);
    }

    #[test]
    fn disabled_service_cannot_restart() {
        let registry = WalApplierRegistry::<1>::default();
        assert!(registry.require(42));
        assert!(registry.try_reserve(42));
        registry.cancel_start(42);
        registry.disable(42);
        assert!(!registry.snapshot(42).unwrap().required);
        assert!(!registry.try_reserve(42));
    }

    #[test]
    fn stale_pid_cannot_release_replacement() {
        let registry = WalApplierRegistry::<1>::default();
        assert!(registry.require(42));
        assert!(registry.try_reserve(42));
        assert!(registry.started(42, 1001));
        registry.stopped(42, 999);
        assert_eq!(registry.snapshot(42).unwrap().pid, 1001);
    }

    #[test]
    fn colliding_databases_probe_to_distinct_entries() {
        let registry = WalApplierRegistry::<4>::default();
        assert!(registry.require(1));
        assert!(registry.require(5));
        assert!(registry.require(9));
        assert!(registry.snapshot(1).is_some());
        assert!(registry.snapshot(5).is_some());
        assert!(registry.snapshot(9).is_some());
    }

    #[test]
    fn wal_batch_contract_is_owned_by_the_mirror_layer() {
        assert_eq!(WAL_APPLY_BATCH_ROWS, 8_192);
    }
}
