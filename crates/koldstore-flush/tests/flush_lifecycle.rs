use koldstore_flush::cleanup;

#[test]
fn hot_cleanup_waits_for_manifest_commit_and_retains_needed_tombstones() {
    assert!(!cleanup::cleanup_allowed(false));
    assert!(cleanup::retain_tombstone(true));

    assert!(cleanup::cleanup_allowed(true));
    assert!(cleanup::retain_tombstone(true));

    assert!(cleanup::cleanup_allowed(true));
    assert!(!cleanup::retain_tombstone(false));
}
