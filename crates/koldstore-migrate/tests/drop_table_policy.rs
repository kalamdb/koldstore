#[test]
fn drop_table_cleanup_plan_records_retain_delete_and_failed_policies() {
    use koldstore_migrate::{
        plan_drop_table_cleanup, DropTableCleanupOutcome, DropTableCleanupPolicy,
        QualifiedTableName,
    };

    let table = QualifiedTableName::parse("app.items").unwrap();

    let retain =
        plan_drop_table_cleanup(table.clone(), 42, DropTableCleanupPolicy::Retain).unwrap();
    assert_eq!(retain.outcome, DropTableCleanupOutcome::MetadataDeactivated);
    assert!(retain
        .statements
        .iter()
        .any(|statement| statement.sql.contains("UPDATE koldstore.schemas")));

    let delete =
        plan_drop_table_cleanup(table.clone(), 42, DropTableCleanupPolicy::Delete).unwrap();
    assert_eq!(
        delete.outcome,
        DropTableCleanupOutcome::DeleteArtifactsQueued
    );
    assert!(delete
        .statements
        .iter()
        .any(|statement| statement.sql.contains("cold_segments")));
    assert!(delete
        .audit_job
        .as_ref()
        .is_some_and(|statement| statement.sql.contains("drop_table_cleanup")));

    let failed = plan_drop_table_cleanup(table, 42, DropTableCleanupPolicy::Failed).unwrap();
    assert_eq!(failed.outcome, DropTableCleanupOutcome::RecoveryRequired);
    assert!(failed
        .audit_job
        .as_ref()
        .is_some_and(|statement| statement.sql.contains("error_trace")));
}
