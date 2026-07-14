use koldstore_common::{CommitSeq, SeqId};
use koldstore_merge::dml::{
    delete_decision, delete_decision_with_flush_fence, DeleteDecision, DmlStamp,
    ManagedDmlOperation,
};
use koldstore::hooks::executor;

#[test]
fn hot_delete_routes_to_physical_delete_or_tombstone_from_cold_hints() {
    assert_eq!(delete_decision(false), DeleteDecision::PhysicalDelete);
    assert_eq!(delete_decision(true), DeleteDecision::Tombstone);
    assert_eq!(
        delete_decision_with_flush_fence(false, true),
        DeleteDecision::Tombstone
    );

    let physical_delete_stamp = DmlStamp::new(
        SeqId::new(10).unwrap(),
        CommitSeq::new(20).unwrap(),
        ManagedDmlOperation::Delete,
    );
    let tombstone_stamp = DmlStamp::new(
        SeqId::new(11).unwrap(),
        CommitSeq::new(21).unwrap(),
        ManagedDmlOperation::Delete,
    );

    assert!(physical_delete_stamp.deleted);
    assert!(tombstone_stamp.deleted);
    assert!(tombstone_stamp.seq > physical_delete_stamp.seq);
    assert!(tombstone_stamp.commit_seq > physical_delete_stamp.commit_seq);
}

#[test]
fn managed_delete_effect_routes_physical_delete_or_tombstone_and_records_mirror_delete() {
    let physical = executor::plan_managed_delete_effect(
        SeqId::new(10).unwrap(),
        CommitSeq::new(20).unwrap(),
        false,
    );
    let tombstone = executor::plan_managed_delete_effect(
        SeqId::new(11).unwrap(),
        CommitSeq::new(21).unwrap(),
        true,
    );

    assert_eq!(physical.stamp.operation, ManagedDmlOperation::Delete);
    assert_eq!(
        physical.mirror_operation,
        koldstore_common::MirrorOperation::Delete
    );
    assert_eq!(physical.manifest_sync_state, "pending_write");
    assert_eq!(
        physical.delete_decision,
        Some(DeleteDecision::PhysicalDelete)
    );
    assert!(physical.stamp.deleted);
    assert!(physical.keeps_one_hot_row_per_pk);

    assert_eq!(tombstone.delete_decision, Some(DeleteDecision::Tombstone));
    assert!(tombstone.stamp.deleted);
    assert!(tombstone.stamp.seq > physical.stamp.seq);
    assert!(tombstone.stamp.commit_seq > physical.stamp.commit_seq);
}
