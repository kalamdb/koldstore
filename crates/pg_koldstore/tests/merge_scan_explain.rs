use pg_koldstore::{hooks::planner, merge_scan};

#[test]
fn merge_scan_explain_and_plan_contract_are_exposed() {
    assert_eq!(planner::MERGE_SCAN_NAME, "KoldstoreMergeScan");
    assert_eq!(merge_scan::path::CUSTOM_PATH_NAME, "KoldstoreMergeScan");

    let plan = merge_scan::plan::MergeScanPlan::new(42, vec!["id".to_string()]);
    assert_eq!(plan.table_oid, 42);
    assert_eq!(plan.primary_key_columns, vec!["id"]);
}
