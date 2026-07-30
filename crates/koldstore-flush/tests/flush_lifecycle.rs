use koldstore_flush::cleanup;

#[test]
fn hot_cleanup_waits_for_manifest_commit_and_retains_needed_tombstones() {
    let before_commit = cleanup::plan_hot_cleanup(false, true);
    let after_commit_with_cold_pk = cleanup::plan_hot_cleanup(true, true);
    let after_commit_without_cold_pk = cleanup::plan_hot_cleanup(true, false);

    assert!(!before_commit.remove_live_hot_rows);
    assert!(before_commit.retain_tombstone);

    assert!(after_commit_with_cold_pk.remove_live_hot_rows);
    assert!(after_commit_with_cold_pk.retain_tombstone);

    assert!(after_commit_without_cold_pk.remove_live_hot_rows);
    assert!(!after_commit_without_cold_pk.retain_tombstone);
}
