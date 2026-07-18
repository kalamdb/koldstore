use koldstore_catalog::{SegmentVisibility, SyncState};

#[test]
fn active_segment_visibility_only_includes_active_segments() {
    assert!(SegmentVisibility::Active.is_query_visible());
    assert!(!SegmentVisibility::Pending.is_query_visible());
    assert!(!SegmentVisibility::Deleted.is_query_visible());
}

#[test]
fn sync_state_after_hot_dml_only_dirties_in_sync() {
    assert_eq!(SyncState::InSync.after_hot_dml(), SyncState::PendingWrite);
    assert_eq!(
        SyncState::PendingWrite.after_hot_dml(),
        SyncState::PendingWrite
    );
    assert_eq!(SyncState::Error.after_hot_dml(), SyncState::Error);
    assert_eq!(SyncState::PendingWrite.as_str(), "pending_write");
    assert!(SyncState::PendingWrite.can_transition_to(SyncState::Syncing));
}
