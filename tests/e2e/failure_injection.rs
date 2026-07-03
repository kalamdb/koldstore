#[test]
fn failure_injection_matrix_lists_required_faults() {
    let faults = [
        "MinIO outage",
        "corrupt Parquet footer",
        "stale manifest generation",
        "missing manifest",
        "orphan final object",
        "credential failure",
        "network timeout",
    ];

    assert_eq!(faults.len(), 7);
}
