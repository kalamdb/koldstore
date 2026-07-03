use pg_koldstore::sql::dml::{delete_decision, DeleteDecision};

#[test]
fn hot_delete_routes_to_physical_delete_or_tombstone_from_cold_hints() {
    assert_eq!(delete_decision(false), DeleteDecision::PhysicalDelete);
    assert_eq!(delete_decision(true), DeleteDecision::Tombstone);
}
