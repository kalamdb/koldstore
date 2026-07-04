#[test]
fn ddl_hook_documents_drop_table_cleanup_policy() {
    let ddl = include_str!("../src/hooks/ddl.rs");

    for policy in ["retain", "delete", "failed"] {
        assert!(ddl.contains(policy), "missing DROP TABLE policy {policy}");
    }

    assert!(ddl.contains("DROP TABLE"));
    assert!(ddl.contains("object artifact"));
}

#[test]
fn drop_table_cleanup_plan_records_retain_delete_and_failed_policies() {
    use pg_koldstore::hooks::ddl::{
        plan_drop_table_cleanup, DropTableCleanupOutcome, DropTableCleanupPolicy,
    };
    use pg_koldstore::migrate::QualifiedTableName;

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
        .any(|statement| statement.sql.contains("drop_table_cleanup")));

    let failed = plan_drop_table_cleanup(table, 42, DropTableCleanupPolicy::Failed).unwrap();
    assert_eq!(failed.outcome, DropTableCleanupOutcome::RecoveryRequired);
    assert!(failed
        .statements
        .iter()
        .any(|statement| statement.sql.contains("error_trace")));
}
