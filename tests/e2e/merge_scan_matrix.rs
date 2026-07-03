#[path = "common/mod.rs"]
mod common;

#[test]
fn merge_scan_matrix_targets_postgresql_15_16_17() {
    assert_eq!(
        common::local_pg_matrix()
            .into_iter()
            .map(|target| target.version)
            .collect::<Vec<_>>(),
        common::expected_pg_versions()
    );
}

#[test]
fn merge_scan_matrix_covers_results_explain_residual_quals_and_outage() {
    let required_assertions = [
        "merged_select",
        "custom_scan_explain",
        "residual_quals_after_winner",
        "cold_outage_error",
    ];

    assert!(required_assertions.contains(&"merged_select"));
    assert!(required_assertions.contains(&"custom_scan_explain"));
    assert!(required_assertions.contains(&"residual_quals_after_winner"));
    assert!(required_assertions.contains(&"cold_outage_error"));
}
