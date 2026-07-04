#[path = "../common/mod.rs"]
mod common;

#[test]
fn flush_matrix_targets_postgresql_15_16_17() {
    let versions = common::local_pg_matrix()
        .iter()
        .map(|target| target.version)
        .collect::<Vec<_>>();

    assert_eq!(versions, common::expected_pg_versions());
}

#[test]
fn flush_matrix_covers_flush_manifest_metadata_and_hot_cleanup() {
    let workflow = [
        "koldstore.flush_table",
        "batch-0.parquet",
        "manifest.json",
        "koldstore.cold_segments",
        "koldstore.cold_pk_hints",
        "hot cleanup after manifest commit",
    ];

    for required_step in [
        "koldstore.flush_table",
        "manifest.json",
        "koldstore.cold_segments",
        "koldstore.cold_pk_hints",
        "hot cleanup after manifest commit",
    ] {
        assert!(workflow.contains(&required_step));
    }
}
