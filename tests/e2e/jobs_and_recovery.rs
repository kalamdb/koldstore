#[test]
fn jobs_and_recovery_contract_covers_status_retries_and_idempotence() {
    let recovery = pg_koldstore::sql::ops::recover_segments_plan(
        Some(koldstore_core::TableName::parse("app.items").unwrap()),
        false,
    )
    .unwrap();

    assert!(recovery.statement.sql.contains("koldstore.jobs"));
    assert!(recovery.statement.sql.contains("recover_segments"));
    assert!(recovery.statement.sql.contains("attempts"));
    assert!(recovery.statement.sql.contains("dry_run"));
}
