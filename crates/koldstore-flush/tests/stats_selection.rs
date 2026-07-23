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

#[test]
fn force_wave_cap_limits_full_mirror_selection() {
    use koldstore_flush::{apply_force_flush_wave_cap, FORCE_FLUSH_WAVE_ROW_CAP};

    let selection = resolve_force_flush_selection(
        FlushStats {
            row_count: 50_000,
            min_seq: 1,
            max_seq: 50_000,
            min_commit_seq: 1,
            max_commit_seq: 50_000,
        },
        FlushStats::empty(),
    );
    let capped = apply_force_flush_wave_cap(
        selection,
        FORCE_FLUSH_WAVE_ROW_CAP,
        Some((FORCE_FLUSH_WAVE_ROW_CAP, FORCE_FLUSH_WAVE_ROW_CAP)),
    );
    assert_eq!(capped.stats.row_count, FORCE_FLUSH_WAVE_ROW_CAP);
}

#[test]
fn catchup_wave_stops_before_post_watermark_rows() {
    use koldstore_flush::{should_continue_flush_catchup, should_start_catchup_wave};

    let upto = Some(10_000);
    assert!(should_start_catchup_wave(upto, 10_000, 1));
    assert!(should_continue_flush_catchup(upto, 5_000));
    assert!(!should_continue_flush_catchup(upto, 10_000));
    // Concurrent fence-applied rows always receive higher seq values.
    assert!(!should_start_catchup_wave(upto, 50, 10_001));
    // Empty start snapshot: allow one wave, then stop looping.
    assert!(should_start_catchup_wave(None, 10, 1));
    assert!(!should_continue_flush_catchup(None, 10));
}
