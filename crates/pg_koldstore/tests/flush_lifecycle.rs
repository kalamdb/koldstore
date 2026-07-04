use pg_koldstore::flush::{cleanup, job, worker};

#[test]
fn hot_cleanup_waits_for_manifest_commit_and_retains_needed_tombstones() {
    let before_commit = cleanup::plan_hot_cleanup(false, true);
    let after_commit_with_cold_pk = cleanup::plan_hot_cleanup(true, true);
    let after_commit_without_cold_pk = cleanup::plan_hot_cleanup(true, false);

    assert!(!before_commit.remove_live_hot_rows);
    assert!(before_commit.retain_tombstone);

    assert!(after_commit_with_cold_pk.remove_live_hot_rows);
    assert!(after_commit_with_cold_pk.retain_tombstone);

    assert!(after_commit_without_cold_pk.remove_live_hot_rows);
    assert!(!after_commit_without_cold_pk.retain_tombstone);
}

#[test]
fn flush_worker_registration_documents_builtin_and_sql_fallback_boundaries() {
    let modes = worker::flush_worker_modes();

    assert!(worker::requires_shared_preload());
    assert!(modes.contains(&worker::FlushWorkerMode::BuiltInBackgroundWorker));
    assert!(modes.contains(&worker::FlushWorkerMode::SqlFunctionFallback));
    assert!(modes.contains(&worker::FlushWorkerMode::PgCronFallback));
}

#[test]
fn zero_sized_flush_batch_does_not_request_another_scan() {
    assert!(!job::should_continue_batch(0, 0));
    assert!(!job::should_continue_batch(10, 0));
}
