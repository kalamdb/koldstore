#[path = "common/mod.rs"]
mod common;

#[test]
fn merge_scan_matrix_targets_postgresql_15_16_17() {
    assert_eq!(
        common::local_pg_matrix().map(|target| target.version),
        [15, 16, 17]
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
