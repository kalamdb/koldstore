//! Sync-state ownership lives in `koldstore-manifest`; flush re-exports it as
//! `ManifestSyncState` for job-phase callers.

use koldstore_flush::job::ManifestSyncState;
use koldstore_manifest::SyncState;

#[test]
fn flush_manifest_sync_state_is_manifest_crate_sync_state() {
    assert_eq!(
        ManifestSyncState::PendingWrite.as_str(),
        SyncState::PendingWrite.as_str()
    );
    assert_eq!(
        ManifestSyncState::PendingWrite.start_flush(),
        SyncState::Syncing
    );
}
