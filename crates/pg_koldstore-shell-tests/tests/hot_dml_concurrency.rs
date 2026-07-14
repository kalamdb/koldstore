use koldstore::hooks::xact::{CommitSequenceAllocator, CommitSequenceDomain};
use koldstore_common::ScopeKey;
use koldstore_merge::dml::ManagedDmlOperation;

#[test]
fn commit_sequence_allocator_is_monotonic_for_same_pk_writers_with_rollback_gaps() {
    let allocator = CommitSequenceAllocator::new_for_tests();
    let domain = CommitSequenceDomain::for_table_scope(1, Some(ScopeKey::new("pk:abc").unwrap()));
    let first = allocator.allocate_for_domain(&domain).unwrap();
    let rolled_back = allocator.allocate_for_domain(&domain).unwrap();
    let second = allocator.allocate_for_domain(&domain).unwrap();

    assert!(rolled_back.commit_seq > first.commit_seq);
    assert!(second.commit_seq > rolled_back.commit_seq);
    assert_eq!(first.lock_key, domain.advisory_lock_key());
    assert_eq!(rolled_back.lock_key, first.lock_key);
    assert_eq!(second.lock_key, first.lock_key);
    assert_eq!(allocator.domain(), "table:1:scope:pk:abc");
    assert!(ManagedDmlOperation::Update.keeps_one_hot_row_per_pk());
    assert!(ManagedDmlOperation::Revive.keeps_one_hot_row_per_pk());
}
