use koldstore_merge::dml::{delete_decision_with_flush_fence, DeleteDecision};

#[test]
fn delete_racing_with_flush_is_tombstoned_even_without_existing_cold_hint() {
    assert_eq!(
        delete_decision_with_flush_fence(false, true),
        DeleteDecision::Tombstone
    );
    assert_eq!(
        delete_decision_with_flush_fence(false, false),
        DeleteDecision::PhysicalDelete
    );
    assert_eq!(
        delete_decision_with_flush_fence(true, false),
        DeleteDecision::Tombstone
    );
}
