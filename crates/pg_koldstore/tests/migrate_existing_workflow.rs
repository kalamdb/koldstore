use pg_koldstore::{
    migrate::{
        jobs::MigrationJobPhase,
        order::{CatalogColumn, CatalogPrimaryKey, OrderingSource},
        plan_existing_table_migration, ExistingTableCatalog, MigrationTableContext,
    },
    sql::ddl::MigrateTableRequest,
};
use uuid::Uuid;

fn request(options: serde_json::Value) -> MigrateTableRequest {
    MigrateTableRequest {
        table_name: "app.items".to_string(),
        table_type: "shared".to_string(),
        storage_name: "local".to_string(),
        flush_policy: Some("rows:1000,interval:60".to_string()),
        scope_column: None,
        options,
    }
}

fn context() -> MigrationTableContext {
    MigrationTableContext {
        table_oid: 42,
        storage_id: Uuid::from_u128(7),
    }
}

#[test]
fn existing_table_migration_plan_prepares_async_mirror_initialization_job() {
    let catalog = ExistingTableCatalog {
        primary_key: CatalogPrimaryKey::single("id"),
        indexed_columns: vec!["body".to_string()],
        columns: vec![
            CatalogColumn::bigint("id")
                .primary_key()
                .default_expr("nextval('items_id_seq'::regclass)"),
            CatalogColumn::text("body"),
        ],
    };

    let plan = plan_existing_table_migration(
        &request(serde_json::json!({ "backfill_batch_size": 2_048 })),
        context(),
        catalog,
        Uuid::from_u128(99),
    )
    .unwrap();

    assert_eq!(plan.table_oid, 42);
    assert_eq!(plan.storage_id, Uuid::from_u128(7));
    assert_eq!(plan.ordering.column, "id");
    assert_eq!(
        plan.ordering.source,
        OrderingSource::AutoIncrementPrimaryKey
    );
    assert_eq!(plan.backfill_batch_size.get(), 2_048);
    assert_eq!(plan.initial_phase, MigrationJobPhase::InitializeMirror);
    assert!(plan
        .backfill_job
        .statement
        .sql
        .contains("'migrate_backfill'"));
    assert!(plan
        .backfill_job
        .statement
        .sql
        .contains("'initialize_mirror'"));
    assert!(!plan
        .backfill_job
        .statement
        .sql
        .contains("'add_system_columns'"));
    assert_eq!(plan.backfill_job.payload["phase"], "initialize_mirror");
    assert_eq!(
        plan.backfill_job.payload["flush_policy"],
        "rows:1000,interval:60"
    );
}

#[test]
fn existing_table_migration_plan_accepts_explicit_order_column_from_options() {
    let catalog = ExistingTableCatalog {
        primary_key: CatalogPrimaryKey::single("id"),
        indexed_columns: vec!["created_at".to_string()],
        columns: vec![
            CatalogColumn::uuid("id").primary_key(),
            CatalogColumn::timestamp("created_at"),
        ],
    };

    let plan = plan_existing_table_migration(
        &request(serde_json::json!({ "order_column": "created_at" })),
        context(),
        catalog,
        Uuid::from_u128(100),
    )
    .unwrap();

    assert_eq!(plan.ordering.column, "created_at");
    assert_eq!(plan.ordering.source, OrderingSource::ExplicitColumn);
}

#[test]
fn existing_table_migration_plan_rejects_existing_rows_without_stable_ordering() {
    let catalog = ExistingTableCatalog {
        primary_key: CatalogPrimaryKey::single("id"),
        indexed_columns: Vec::new(),
        columns: vec![CatalogColumn::uuid("id").primary_key()],
    };

    let error = plan_existing_table_migration(
        &request(serde_json::json!({})),
        context(),
        catalog,
        Uuid::from_u128(101),
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "existing table migration requires an auto-increment primary key or explicit order column"
    );
}
