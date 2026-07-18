//! In-batch primary-key dedupe for latest-state mirror apply.
//!
//! Caps memory at a fixed row budget and forces a flush when the same PK would
//! otherwise appear twice in one batch (so upsert order stays well-defined).

use std::collections::HashSet;
use std::hash::Hash;

/// Default maximum rows retained in one apply batch before flush.
pub const APPLY_BATCH_ROWS: usize = 8_192;

/// Decision for whether the current batch must flush before accepting a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchFlushReason {
    /// Operation or relation key changed.
    KeyChanged,
    /// Batch reached [`APPLY_BATCH_ROWS`].
    CapReached,
    /// Same primary-key identity already present in this batch.
    DuplicateIdentity,
}

/// Returns why the batch must flush before accepting `identity`, if at all.
///
/// # Invariants
///
/// - Empty batch never needs a flush for capacity/identity alone when `same_key`.
/// - Duplicate identity always flushes so the prior row is durable before the
///   next mutation for the same PK is staged.
#[must_use]
pub fn must_flush_before_push<K: PartialEq, I: Eq + Hash>(
    batch_key: Option<&K>,
    next_key: &K,
    row_count: usize,
    seen: &HashSet<I>,
    identity: &I,
    batch_cap: usize,
) -> Option<BatchFlushReason> {
    let current_key = batch_key?;
    if current_key != next_key {
        return Some(BatchFlushReason::KeyChanged);
    }
    if row_count >= batch_cap {
        return Some(BatchFlushReason::CapReached);
    }
    if seen.contains(identity) {
        return Some(BatchFlushReason::DuplicateIdentity);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{must_flush_before_push, BatchFlushReason, APPLY_BATCH_ROWS};
    use std::collections::HashSet;

    #[test]
    fn empty_batch_accepts_first_row() {
        let seen = HashSet::<String>::new();
        assert_eq!(
            must_flush_before_push::<&str, String>(None, &"insert", 0, &seen, &"1".into(), 8),
            None
        );
    }

    #[test]
    fn duplicate_identity_forces_flush() {
        let mut seen = HashSet::new();
        seen.insert("1".to_string());
        assert_eq!(
            must_flush_before_push(Some(&"insert"), &"insert", 1, &seen, &"1".to_string(), 8),
            Some(BatchFlushReason::DuplicateIdentity)
        );
    }

    #[test]
    fn key_change_forces_flush() {
        let seen = HashSet::<String>::new();
        assert_eq!(
            must_flush_before_push(Some(&"insert"), &"update", 1, &seen, &"1".to_string(), 8),
            Some(BatchFlushReason::KeyChanged)
        );
    }

    #[test]
    fn cap_forces_flush() {
        let seen = HashSet::<String>::new();
        assert_eq!(
            must_flush_before_push(
                Some(&"insert"),
                &"insert",
                APPLY_BATCH_ROWS,
                &seen,
                &"1".to_string(),
                APPLY_BATCH_ROWS
            ),
            Some(BatchFlushReason::CapReached)
        );
    }
}
