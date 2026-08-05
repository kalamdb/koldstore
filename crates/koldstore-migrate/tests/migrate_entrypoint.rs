use koldstore_common::SqlAccess as SpiAccess;
use koldstore_common::{ManageTableOptions, StorageId, TableName, TableOid};
use koldstore_migrate::MigrateTableRequest;
use koldstore_migrate::{plan_empty_table_migration, MigrationError, MigrationTableContext};
fn shared_request() -> MigrateTableRequest {
    MigrateTableRequest {
        table_name: TableName::parse("app.items").unwrap(),
        table_type: "shared".to_string(),
        storage_name: "local-minio".to_string(),
        scope_column: None,
        options: ManageTableOptions::default().with_flush(1000, 1, 1000),
    }
}

fn context() -> MigrationTableContext {
    MigrationTableContext {
        table_oid: TableOid::from_raw(42),
        storage_id: StorageId::new("00000007").unwrap(),
    }
}

#[test]
fn shared_migrate_table_plan_validates_and_probes_empty_table() {
    let plan = plan_empty_table_migration(&shared_request(), context()).unwrap();

    assert_eq!(plan.table_oid.get(), 42);
    assert_eq!(plan.storage_id.as_str(), "00000007");
    assert_eq!(plan.table.schema.as_deref(), Some("app"));
    assert_eq!(plan.table.name, "items");
    assert_eq!(plan.effective_scope_column, None);
    assert_eq!(plan.empty_table_probe.operation, "check empty table");
    assert_eq!(plan.empty_table_probe.access, SpiAccess::ReadOnly);
    assert_eq!(
        plan.empty_table_probe.sql,
        "SELECT 1 FROM ONLY \"app\".\"items\" LIMIT 1"
    );
}

#[test]
fn user_migrate_table_plan_requires_application_scope_column() {
    let mut request = shared_request();
    request.table_name = TableName::parse("notes").unwrap();
    request.table_type = "user".to_string();

    let error = plan_empty_table_migration(&request, context()).unwrap_err();
    assert_eq!(error, MigrationError::MissingScopeColumn);

    request.scope_column = Some("user_id".to_string());
    let plan = plan_empty_table_migration(&request, context()).unwrap();

    assert_eq!(plan.table.schema, None);
    assert_eq!(plan.table.name, "notes");
    assert_eq!(plan.effective_scope_column.as_deref(), Some("user_id"));
    assert_eq!(
        plan.empty_table_probe.sql,
        "SELECT 1 FROM ONLY \"notes\" LIMIT 1"
    );
}

#[test]
fn migrate_table_plan_rejects_unsupported_or_unsafe_arguments() {
    let mut request = shared_request();
    request.table_type = "archive".to_string();
    assert!(plan_empty_table_migration(&request, context()).is_err());

    assert!(TableName::parse("app.items; drop table app.items").is_err());

    request = shared_request();
    request.storage_name = " ".to_string();
    assert!(plan_empty_table_migration(&request, context()).is_err());

    request = shared_request();
    request.table_type = "user".to_string();
    request.scope_column = Some("not safe".to_string());
    assert!(plan_empty_table_migration(&request, context()).is_err());
}
