use koldstore_merge::dml::{allocate_seq_for_tests, ManagedDmlOperation};

#[test]
fn seq_allocator_is_monotonic_across_same_pk_writers() {
    let first = allocate_seq_for_tests().unwrap();
    let rolled_back = allocate_seq_for_tests().unwrap();
    let second = allocate_seq_for_tests().unwrap();

    assert!(rolled_back > first);
    assert!(second > rolled_back);
    assert!(ManagedDmlOperation::Update.keeps_one_hot_row_per_pk());
    assert!(ManagedDmlOperation::Revive.keeps_one_hot_row_per_pk());
}
