use koldstore_flush::job::ManifestSyncState;

#[test]
fn manifest_cache_state_transitions_cover_flush_lifecycle() {
    assert_eq!(ManifestSyncState::PendingWrite.as_str(), "pending_write");
    assert_eq!(
        ManifestSyncState::PendingWrite.start_flush(),
        ManifestSyncState::Syncing
    );
    assert_eq!(
        ManifestSyncState::Syncing.finish_success(false),
        ManifestSyncState::InSync
    );
    assert_eq!(
        ManifestSyncState::Syncing.finish_success(true),
        ManifestSyncState::PendingWrite
    );
    assert_eq!(
        ManifestSyncState::Syncing.finish_error(),
        ManifestSyncState::Error
    );
    assert_eq!(ManifestSyncState::Stale.as_str(), "stale");
}
