use koldstore_flush::policy::{flush_rows_for_excess, policy_flush_row_count, FlushPolicy};

#[test]
fn structured_schema_options_load_hot_row_limit_policy() {
    let policy = FlushPolicy::from_value(&serde_json::json!({
        "hot_row_limit": 10_000,
        "min_flush_rows": 1_000,
        "max_rows_per_file": 500,
    }))
    .unwrap();

    assert_eq!(policy.hot_row_limit, Some(10_000));
    assert_eq!(policy.min_flush_rows, Some(1_000));
    assert_eq!(policy.max_rows_per_file, Some(500));
}

#[test]
fn flush_rows_for_excess_honors_min_flush_rows_threshold() {
    assert_eq!(flush_rows_for_excess(505, 1_000), 0);
    assert_eq!(flush_rows_for_excess(1_000, 1_000), 1_000);
    assert_eq!(flush_rows_for_excess(1_250, 1_000), 1_000);
    assert_eq!(flush_rows_for_excess(1_500, 1_000), 1_500);
}

#[test]
fn policy_flush_row_count_honors_hot_row_limit_and_min_flush_rows() {
    let policy = FlushPolicy {
        hot_row_limit: Some(25_000),
        min_flush_rows: Some(300),
        max_rows_per_file: None,
        target_file_size_mb: None,
        segment_row_threshold: None,
    };
    assert_eq!(policy_flush_row_count(50_000, &policy), 24_900);
    assert_eq!(policy_flush_row_count(25_000, &policy), 0);
}

#[test]
fn policy_flush_row_count_chunks_large_excess_like_row_selection_did() {
    let policy = FlushPolicy {
        hot_row_limit: Some(10_000),
        min_flush_rows: Some(1_000),
        max_rows_per_file: Some(500),
        target_file_size_mb: None,
        segment_row_threshold: None,
    };
    assert_eq!(policy_flush_row_count(11_250, &policy), 1_000);
}
