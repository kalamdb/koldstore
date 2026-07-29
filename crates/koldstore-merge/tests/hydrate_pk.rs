#[test]
fn sql_exposes_hydrate_pk_api() {
    use koldstore_merge::dml::plan_hydrate_pk;

    assert!(koldstore_merge::dml::COLD_DML_FUNCTIONS.contains(&"koldstore.hydrate_pk"));

    let result = plan_hydrate_pk(true);
    assert_eq!(result.affected_rows, 1);
    assert!(result.cold_lookup_performed);
    assert!(!result.tombstone_written);
}
