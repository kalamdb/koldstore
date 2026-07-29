//! Sync-state ownership lives in `koldstore-catalog`.

use koldstore_catalog::SyncState;

#[test]
fn pending_write_transitions_into_syncing() {
    assert_eq!(SyncState::PendingWrite.as_str(), "pending_write");
    assert_eq!(SyncState::PendingWrite.start_flush(), SyncState::Syncing);
}
