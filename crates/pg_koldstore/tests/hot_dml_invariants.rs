use koldstore_core::{CommitSeq, SeqId};
use pg_koldstore::{hooks::executor, sql::dml};

#[test]
fn dml_helpers_keep_one_hot_row_per_pk_by_using_upsert_revival() {
    let insert = dml::ManagedDmlOperation::Insert;
    let revive = dml::ManagedDmlOperation::Revive;

    assert!(insert.keeps_one_hot_row_per_pk());
    assert!(revive.keeps_one_hot_row_per_pk());
    assert_eq!(
        executor::plan_mirror_capture_effect(revive).operation,
        koldstore_core::MirrorOperation::Insert
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
        koldstore_core::MirrorOperation::Insert
    );
    assert_eq!(effect.manifest_sync_state, "pending_write");
    assert_eq!(effect.delete_decision, None);
    assert!(!effect.stamp.deleted);
    assert!(effect.keeps_one_hot_row_per_pk);
}
