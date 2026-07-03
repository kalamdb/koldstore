use pg_koldstore::sql::dml::{DmlStamp, ManagedDmlOperation};

#[test]
fn hot_update_stamp_advances_seq_and_commit_seq() {
    let stamp = DmlStamp::new(10, 20, ManagedDmlOperation::Update).unwrap();

    assert_eq!(stamp.seq.get(), 10);
    assert_eq!(stamp.commit_seq.get(), 20);
    assert_eq!(stamp.operation, ManagedDmlOperation::Update);
    assert!(!stamp.deleted);
}
