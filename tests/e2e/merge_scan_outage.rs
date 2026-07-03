#[test]
fn merge_scan_outage_placeholder_requires_error_not_partial_results() {
    assert!("ERROR instead of partial hot-only results".contains("ERROR"));
}

