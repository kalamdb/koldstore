#[test]
fn flush_object_outage_keeps_hot_authoritative_and_records_error_job_state() {
    use pg_koldstore::flush::job::{FlushFailurePlan, ManifestSyncState};

    let plan = FlushFailurePlan::object_store_outage("s3 timeout");

    assert_eq!(plan.next_manifest_state, ManifestSyncState::Error);
    assert!(plan.hot_data_authoritative);
    assert_eq!(plan.job_state, "error");
    assert_eq!(plan.last_error.as_deref(), Some("s3 timeout"));
}
