//! Process-local scope row counters for flush initiation.
//!
//! DML bumps counters keyed by [`ScopeCounterKey`]. Pre-flush reads keys for a
//! table and creates `pending` catalog segments for thresholds. Counters are
//! advisory; [`ScopeCounters::reconcile`] rebuilds from durable mirror/hot
//! counts after restart.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

use koldstore_common::ScopeCounterKey;

/// Process-wide in-memory counter map.
fn counters() -> &'static Mutex<HashMap<ScopeCounterKey, u64>> {
    static COUNTERS: OnceLock<Mutex<HashMap<ScopeCounterKey, u64>>> = OnceLock::new();
    COUNTERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn test_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// In-memory `(table, Optional<scope>)` row counters.
#[derive(Debug, Default, Clone, Copy)]
pub struct ScopeCounters;

impl ScopeCounters {
    /// Serializes tests that mutate the process-wide counter map.
    pub fn lock_for_tests() -> MutexGuard<'static, ()> {
        test_mutex().lock().expect("scope counter test lock")
    }
    /// Increments the counter for `key` by `delta` (no-op when `delta == 0`).
    pub fn bump(key: ScopeCounterKey, delta: u64) {
        if delta == 0 {
            return;
        }
        let mut map = counters().lock().expect("scope counter lock");
        let count = map.entry(key).or_insert(0);
        *count = count.saturating_add(delta);
    }

    /// Returns the current counter for `key`.
    #[must_use]
    pub fn get(key: &ScopeCounterKey) -> u64 {
        counters()
            .lock()
            .expect("scope counter lock")
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    /// Returns all counter keys for `table_oid` with their counts.
    #[must_use]
    pub fn keys_for_table(table_oid: u32) -> Vec<(ScopeCounterKey, u64)> {
        counters()
            .lock()
            .expect("scope counter lock")
            .iter()
            .filter(|(key, _)| key.table_oid == table_oid)
            .map(|(key, count)| (key.clone(), *count))
            .collect()
    }

    /// Keys at or above `threshold` for `table_oid`.
    #[must_use]
    pub fn keys_at_or_above_threshold(table_oid: u32, threshold: u64) -> Vec<(ScopeCounterKey, u64)> {
        if threshold == 0 {
            return Vec::new();
        }
        Self::keys_for_table(table_oid)
            .into_iter()
            .filter(|(_, count)| *count >= threshold)
            .collect()
    }

    /// Subtracts `amount` from `key` after a pending segment was created (or on drain).
    pub fn consume(key: &ScopeCounterKey, amount: u64) {
        if amount == 0 {
            return;
        }
        let mut map = counters().lock().expect("scope counter lock");
        let Some(entry) = map.get_mut(key) else {
            return;
        };
        *entry = entry.saturating_sub(amount);
        if *entry == 0 {
            map.remove(key);
        }
    }

    /// Replaces the in-memory count for `key` with a durable reconcile value.
    pub fn reconcile(key: ScopeCounterKey, durable_count: u64) {
        use std::collections::hash_map::Entry;

        let mut map = counters().lock().expect("scope counter lock");
        match map.entry(key) {
            Entry::Occupied(occupied) if durable_count == 0 => {
                occupied.remove();
            }
            Entry::Vacant(_) if durable_count == 0 => {}
            Entry::Occupied(mut occupied) => {
                *occupied.get_mut() = durable_count;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(durable_count);
            }
        }
    }

    /// When the in-memory map has no keys for `table_oid`, seed from
    /// `durable_by_scope` (catalog `scope_key` text → count). Shared uses `""`.
    pub fn reconcile_table_if_empty(table_oid: u32, durable_by_scope: &[(String, u64)]) {
        let mut map = counters().lock().expect("scope counter lock");
        let has_any = map.keys().any(|key| key.table_oid == table_oid);
        if has_any {
            return;
        }
        for (scope_key, count) in durable_by_scope {
            if *count == 0 {
                continue;
            }
            let key = if scope_key.is_empty() {
                ScopeCounterKey::shared(table_oid)
            } else if let Ok(key) = ScopeCounterKey::scoped(table_oid, scope_key) {
                key
            } else {
                continue;
            };
            map.insert(key, *count);
        }
    }

    /// Clears all counters. Intended for unit/integration tests only.
    pub fn clear_all_for_tests() {
        counters().lock().expect("scope counter lock").clear();
    }
}
