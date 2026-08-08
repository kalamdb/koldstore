//! Cluster-wide pause flags for async-mirror worker registration.
//!
//! PostgreSQL advisory locks are database-local, so a session on an e2e worker
//! DB cannot block the shared-preload launcher (connected to `postgres`) with
//! `pg_advisory_lock`. This fixed shared-memory set is visible to every backend
//! and is the coordination point for "keep the applier stopped".

use std::sync::atomic::{AtomicU32, Ordering};

/// Empty slot sentinel (PostgreSQL database OIDs are never zero).
const EMPTY: u32 = 0;

/// Fixed-capacity set of database OIDs whose ensure/register path is paused.
#[derive(Debug)]
pub struct EnsurePauseSet<const N: usize> {
    slots: [AtomicU32; N],
}

impl<const N: usize> Default for EnsurePauseSet<N> {
    fn default() -> Self {
        // AtomicU32 is not Copy in a way that allows `[AtomicU32::new(0); N]` on
        // all toolchains; build via array map.
        Self {
            slots: std::array::from_fn(|_| AtomicU32::new(EMPTY)),
        }
    }
}

impl<const N: usize> EnsurePauseSet<N> {
    /// Marks `database_oid` as paused so launcher/session ensure skip registration.
    ///
    /// Returns `false` when the set is full and the oid was not already paused.
    pub fn pause(&self, database_oid: u32) -> bool {
        if database_oid == EMPTY {
            return false;
        }
        // Fast path: already paused.
        for slot in &self.slots {
            if slot.load(Ordering::Acquire) == database_oid {
                return true;
            }
        }
        for slot in &self.slots {
            match slot.compare_exchange(EMPTY, database_oid, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return true,
                Err(existing) if existing == database_oid => return true,
                Err(_) => continue,
            }
        }
        false
    }

    /// Clears a pause for `database_oid` (idempotent).
    pub fn resume(&self, database_oid: u32) {
        if database_oid == EMPTY {
            return;
        }
        for slot in &self.slots {
            let _ = slot.compare_exchange(database_oid, EMPTY, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    /// Returns whether ensure must skip registration for `database_oid`.
    #[must_use]
    pub fn is_paused(&self, database_oid: u32) -> bool {
        if database_oid == EMPTY {
            return false;
        }
        self.slots
            .iter()
            .any(|slot| slot.load(Ordering::Acquire) == database_oid)
    }
}

#[cfg(test)]
mod tests {
    use super::EnsurePauseSet;

    #[test]
    fn pause_resume_round_trip() {
        let set = EnsurePauseSet::<4>::default();
        assert!(!set.is_paused(42));
        assert!(set.pause(42));
        assert!(set.is_paused(42));
        assert!(set.pause(42), "idempotent pause");
        set.resume(42);
        assert!(!set.is_paused(42));
    }

    #[test]
    fn full_set_rejects_new_oids() {
        let set = EnsurePauseSet::<2>::default();
        assert!(set.pause(1));
        assert!(set.pause(2));
        assert!(!set.pause(3));
        assert!(!set.is_paused(3));
        set.resume(1);
        assert!(set.pause(3));
    }
}
