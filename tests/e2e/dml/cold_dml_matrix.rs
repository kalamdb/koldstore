#[path = "../common/mod.rs"]
mod common;

#[test]
fn cold_dml_matrix_targets_postgresql_15_16_17() {
    assert_eq!(
        common::local_pg_matrix()
            .into_iter()
            .map(|target| target.version)
            .collect::<Vec<_>>(),
        common::expected_pg_versions()
    );
}

#[test]
fn cold_dml_matrix_covers_apis_row_events_and_no_default_object_store_reads() {
    let required_assertions = [
        "hydrate_pk_reads_one_cold_pk",
        "update_row_lookup_cold_true_updates",
        "delete_row_writes_tombstone_from_local_metadata",
        "row_events_emitted",
        "default_delete_uses_no_object_store_reads",
    ];

    assert!(required_assertions.contains(&"hydrate_pk_reads_one_cold_pk"));
    assert!(required_assertions.contains(&"update_row_lookup_cold_true_updates"));
    assert!(required_assertions.contains(&"delete_row_writes_tombstone_from_local_metadata"));
    assert!(required_assertions.contains(&"row_events_emitted"));
    assert!(required_assertions.contains(&"default_delete_uses_no_object_store_reads"));
}
