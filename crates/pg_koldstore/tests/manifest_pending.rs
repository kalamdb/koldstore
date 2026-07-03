#[test]
fn sql_contains_pending_write_manifest_state_for_hot_dml() {
    use koldstore_core::{CommitSeq, SeqId};

    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    let effect = pg_koldstore::hooks::executor::plan_managed_insert_effect(
        SeqId::new(1).unwrap(),
        CommitSeq::new(1).unwrap(),
    );

    assert!(sql.contains("pending_write"));
    assert!(pg_koldstore::flush::job::FLUSH_STATES.contains(&"pending_write"));
    assert_eq!(effect.manifest_sync_state, "pending_write");
    assert!(!sql.contains("manifest.json"));
}

#[test]
fn manifest_cache_state_transitions_cover_flush_lifecycle() {
    use pg_koldstore::flush::job::ManifestSyncState;

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
