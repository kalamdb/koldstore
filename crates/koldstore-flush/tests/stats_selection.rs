use koldstore_common::FlushPolicy;
use koldstore_flush::{
    resolve_force_flush_selection, resolve_policy_flush_selection, FlushStats,
    FORCE_TOMBSTONE_ONLY_CAP,
};

#[test]
fn policy_selection_returns_empty_when_pending_within_hot_limit() {
    let policy = FlushPolicy::new(1_000, 100, 1_000);
    let selection = resolve_policy_flush_selection(500, Some(&policy), None, FlushStats::empty());
    assert_eq!(selection.stats.row_count, 0);
    assert!(selection.mirror_ops.is_none());
}

#[test]
fn policy_selection_uses_cutoff_when_excess_exists() {
    let policy = FlushPolicy::new(100, 50, 1_000);
    let selection =
        resolve_policy_flush_selection(250, Some(&policy), Some((150, 999)), FlushStats::empty());
    assert_eq!(selection.stats.row_count, 150);
    assert_eq!(selection.stats.max_seq, 999);
    assert_eq!(selection.stats.max_commit_seq, 999);
}

#[test]
fn force_selection_prefers_small_tombstone_only_batch() {
    let all = FlushStats {
        row_count: 10_000,
        min_seq: 1,
        max_seq: 10_000,
        min_commit_seq: 1,
        max_commit_seq: 10_000,
    };
    let deletes = FlushStats {
        row_count: FORCE_TOMBSTONE_ONLY_CAP,
        min_seq: 1,
        max_seq: 4_096,
        min_commit_seq: 1,
        max_commit_seq: 4_096,
    };
    let selection = resolve_force_flush_selection(all, deletes);
    assert_eq!(selection.stats.row_count, FORCE_TOMBSTONE_ONLY_CAP);
    assert_eq!(selection.mirror_ops, Some(vec![3]));
}

#[test]
fn force_selection_falls_back_to_full_mirror_when_tombstones_exceed_cap() {
    let all = FlushStats {
        row_count: 20_000,
        min_seq: 1,
        max_seq: 20_000,
        min_commit_seq: 1,
        max_commit_seq: 20_000,
    };
    let deletes = FlushStats {
        row_count: FORCE_TOMBSTONE_ONLY_CAP + 1,
        min_seq: 1,
        max_seq: 5_000,
        min_commit_seq: 1,
        max_commit_seq: 5_000,
    };
    let selection = resolve_force_flush_selection(all, deletes);
    assert_eq!(selection.stats.row_count, 20_000);
    assert!(selection.mirror_ops.is_none());
}
