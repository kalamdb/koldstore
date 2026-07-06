use koldstore_core::{CommitSeq, SeqId};
use pg_koldstore::{
    hooks::executor,
    sql::dml::{allocate_seq_for_tests, stamp_dml_effect, DmlStamp, ManagedDmlOperation},
};

#[test]
fn hot_update_stamp_advances_seq_and_commit_seq() {
    let stamp = DmlStamp::new(
        SeqId::new(10).unwrap(),
        CommitSeq::new(20).unwrap(),
        ManagedDmlOperation::Update,
    );

    assert_eq!(stamp.seq.get(), 10);
    assert_eq!(stamp.commit_seq.get(), 20);
    assert_eq!(stamp.operation, ManagedDmlOperation::Update);
    assert!(!stamp.deleted);
}

#[test]
fn dml_stamp_marks_only_delete_operations_as_deleted() {
    for operation in [
        ManagedDmlOperation::Insert,
        ManagedDmlOperation::Update,
        ManagedDmlOperation::Revive,
    ] {
        let stamp = DmlStamp::new(
            SeqId::new(1).unwrap(),
            CommitSeq::new(1).unwrap(),
            operation,
        );

        assert!(!stamp.deleted, "{operation:?} must keep the hot row live");
    }

    let delete = DmlStamp::new(
        SeqId::new(2).unwrap(),
        CommitSeq::new(2).unwrap(),
        ManagedDmlOperation::Delete,
    );
    assert!(delete.deleted);
}

#[test]
fn dml_stamp_rejects_invalid_sequence_values() {
    assert!(SeqId::new(0).is_err());
    assert!(CommitSeq::new(0).is_err());
    assert!(SeqId::new(-1).is_err());
    assert!(CommitSeq::new(-1).is_err());
}

#[test]
fn managed_update_effect_mutates_live_hot_row_and_records_mirror_update() {
    let effect =
        executor::plan_managed_update_effect(SeqId::new(10).unwrap(), CommitSeq::new(20).unwrap());

    assert_eq!(effect.stamp.operation, ManagedDmlOperation::Update);
    assert_eq!(
        effect.mirror_operation,
        koldstore_core::MirrorOperation::Update
    );
    assert_eq!(effect.manifest_sync_state, "pending_write");
    assert_eq!(effect.delete_decision, None);
    assert!(!effect.stamp.deleted);
    assert!(effect.keeps_one_hot_row_per_pk);
}

#[test]
fn seq_allocator_and_stamp_helper_assign_monotonic_row_effect_versions() {
    let first_seq = allocate_seq_for_tests().unwrap();
    let second_seq = allocate_seq_for_tests().unwrap();
    let commit_seq = CommitSeq::new(30).unwrap();
    let stamp = stamp_dml_effect(second_seq, commit_seq, ManagedDmlOperation::Update);

    assert!(second_seq > first_seq);
    assert_eq!(stamp.seq, second_seq);
    assert_eq!(stamp.commit_seq, commit_seq);
    assert_eq!(stamp.operation, ManagedDmlOperation::Update);
    assert!(!stamp.deleted);
}
