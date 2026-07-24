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
    assert!(retain.audit_job.is_none());

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
    let delete_audit = delete.audit_job.expect("delete policy emits audit job");
    assert!(delete_audit.sql.contains("drop_table_cleanup"));
    assert!(
        delete_audit
            .sql
            .contains("jsonb_build_object('policy', 'delete')"),
        "delete audit payload must record policy=delete: {}",
        delete_audit.sql
    );

    let failed = plan_drop_table_cleanup(table, 42, DropTableCleanupPolicy::Failed).unwrap();
    assert_eq!(failed.outcome, DropTableCleanupOutcome::RecoveryRequired);
    let failed_audit = failed.audit_job.expect("failed policy emits audit job");
    assert!(failed_audit.sql.contains("error_trace"));
    assert!(
        failed_audit
            .sql
            .contains("jsonb_build_object('policy', 'failed')"),
        "failed audit payload must record policy=failed, not delete: {}",
        failed_audit.sql
    );
    assert!(!failed_audit.sql.contains("'policy', 'delete'"));
}
