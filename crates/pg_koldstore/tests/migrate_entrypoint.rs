use pg_koldstore::migrate::{
    plan_empty_table_migration, MigrationTableContext, SYSTEM_SCOPE_COLUMN,
};
use pg_koldstore::spi::SpiAccess;
use pg_koldstore::sql::ddl::MigrateTableRequest;
use uuid::Uuid;

fn shared_request() -> MigrateTableRequest {
    MigrateTableRequest {
        table_name: "app.items".to_string(),
        table_type: "shared".to_string(),
        storage_name: "local-minio".to_string(),
        flush_policy: Some("rows:1000,interval:60".to_string()),
        scope_column: None,
        options: serde_json::json!({}),
    }
}

fn context() -> MigrationTableContext {
    MigrationTableContext {
        table_oid: 42,
        storage_id: Uuid::from_u128(7),
    }
}

#[test]
fn shared_migrate_table_plan_validates_and_probes_empty_table() {
    let plan = plan_empty_table_migration(&shared_request(), context()).unwrap();

    assert_eq!(plan.table_oid, 42);
    assert_eq!(plan.storage_id, Uuid::from_u128(7));
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
fn user_migrate_table_plan_defaults_scope_column() {
    let mut request = shared_request();
    request.table_name = "notes".to_string();
    request.table_type = "user".to_string();

    let plan = plan_empty_table_migration(&request, context()).unwrap();

    assert_eq!(plan.table.schema, None);
    assert_eq!(plan.table.name, "notes");
    assert_eq!(
        plan.effective_scope_column.as_deref(),
        Some(SYSTEM_SCOPE_COLUMN)
    );
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

    request = shared_request();
    request.table_name = "app.items; drop table app.items".to_string();
    assert!(plan_empty_table_migration(&request, context()).is_err());

    request = shared_request();
    request.storage_name = " ".to_string();
    assert!(plan_empty_table_migration(&request, context()).is_err());

    request = shared_request();
    request.table_type = "user".to_string();
    request.scope_column = Some("not safe".to_string());
    assert!(plan_empty_table_migration(&request, context()).is_err());
}
