use pg_koldstore::merge_scan::exec::{begin_merge_scan, ColdAvailability, MergeScanError};

#[test]
fn merge_scan_outage_requires_error_not_partial_hot_only_results() {
    let error = begin_merge_scan(
        42,
        vec!["app/items/batch-0.parquet".to_string()],
        ColdAvailability::Unavailable,
    )
    .unwrap_err();

    assert_eq!(error, MergeScanError::ColdRequiredUnavailable);
    assert!(error.to_string().contains("cold data required"));
}
