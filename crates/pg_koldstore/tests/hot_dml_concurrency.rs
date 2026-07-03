use pg_koldstore::hooks::xact::CommitSequenceAllocator;

#[test]
fn commit_sequence_allocator_is_monotonic_for_same_pk_writers() {
    let allocator = CommitSequenceAllocator::new_for_tests();
    let first = allocator.allocate_for_domain("table:1:pk:abc").unwrap();
    let second = allocator.allocate_for_domain("table:1:pk:abc").unwrap();

    assert!(second > first);
}
