use koldstore::hooks::{executor, xact};
use koldstore_common::{CommitSeq, MirrorOperation, SeqId};
use koldstore_merge::dml;

#[test]
fn dml_helpers_keep_one_hot_row_per_pk_by_using_upsert_revival() {
    let insert = dml::ManagedDmlOperation::Insert;
    let revive = dml::ManagedDmlOperation::Revive;

    assert!(insert.keeps_one_hot_row_per_pk());
    assert!(revive.keeps_one_hot_row_per_pk());
    assert_eq!(
        executor::plan_mirror_capture_effect(revive).operation,
        koldstore_common::MirrorOperation::Insert
    );
    assert!(dml::ManagedDmlOperation::Update.keeps_one_hot_row_per_pk());
    assert!(dml::ManagedDmlOperation::Delete.keeps_one_hot_row_per_pk());
    assert!(executor::managed_dml_hook_names().contains(&"INSERT"));
    assert!(executor::managed_dml_hook_names().contains(&"UPDATE"));
    assert!(executor::managed_dml_hook_names().contains(&"DELETE"));
}

#[test]
fn managed_insert_effect_stamps_live_mirror_operation_and_pending_manifest() {
    let effect =
        executor::plan_managed_insert_effect(SeqId::new(10).unwrap(), CommitSeq::new(20).unwrap());

    assert_eq!(effect.stamp.operation, dml::ManagedDmlOperation::Insert);
    assert_eq!(
        effect.mirror_operation,
        koldstore_common::MirrorOperation::Insert
    );
    assert_eq!(effect.manifest_sync_state, "pending_write");
    assert_eq!(effect.delete_decision, None);
    assert!(!effect.stamp.deleted);
    assert!(effect.keeps_one_hot_row_per_pk);
}

#[test]
fn executor_maps_user_dml_to_latest_state_mirror_operations() {
    let insert = executor::plan_mirror_capture_effect(dml::ManagedDmlOperation::Insert);
    let update = executor::plan_mirror_capture_effect(dml::ManagedDmlOperation::Update);
    let delete = executor::plan_mirror_capture_effect(dml::ManagedDmlOperation::Delete);
    let revive = executor::plan_mirror_capture_effect(dml::ManagedDmlOperation::Revive);

    assert_eq!(insert.operation, MirrorOperation::Insert);
    assert_eq!(update.operation, MirrorOperation::Update);
    assert_eq!(delete.operation, MirrorOperation::Delete);
    assert_eq!(revive.operation, MirrorOperation::Insert);
    for effect in [insert, update, delete, revive] {
        assert_eq!(effect.seq_expression, "SNOWFLAKE_ID()");
        assert_eq!(effect.commit_lsn_expression, "pg_current_wal_lsn()");
        assert!(effect.transactional);
    }
}

#[test]
fn mirror_capture_scope_rolls_back_with_user_transaction() {
    let scope = xact::mirror_capture_transaction_scope();

    assert_eq!(
        scope,
        xact::MirrorCaptureTransactionScope::SameUserTransaction
    );
    assert!(xact::mirror_capture_rolls_back_with_user_transaction(scope));
}
