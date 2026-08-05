use koldstore_common::{ColumnId, ColumnRef, ManageTableOptions, StorageId, TableName, TableOid};
use koldstore_migrate::{
    jobs::MigrationJobPhase,
    order::{CatalogColumn, CatalogPrimaryKey, OrderingSource},
    plan_existing_table_migration, ExistingTableCatalog, MigrateTableRequest,
    MigrationTableContext,
};
use uuid::Uuid;

fn request(options: ManageTableOptions) -> MigrateTableRequest {
    MigrateTableRequest {
        table_name: TableName::parse("app.items").unwrap(),
        table_type: "shared".to_string(),
        storage_name: "local".to_string(),
        scope_column: None,
        options,
    }
}

fn context() -> MigrationTableContext {
    MigrationTableContext {
        table_oid: TableOid::from_raw(42),
        storage_id: StorageId::new("00000007").unwrap(),
    }
}

#[test]
fn existing_table_migration_plan_prepares_async_mirror_initialization_job() {
    let catalog = ExistingTableCatalog::new(
        CatalogPrimaryKey::single(1, "id"),
        vec![
            CatalogColumn::bigint(1, "id")
                .primary_key()
                .default_expr("nextval('items_id_seq'::regclass)"),
            CatalogColumn::text(2, "body"),
        ],
        vec![ColumnRef::new(ColumnId::from_attnum(2), "body")],
    );

    let plan = plan_existing_table_migration(
        &request({
            let mut options = ManageTableOptions::default().with_flush(1000, 1, 1000);
            options.backfill_batch_size = Some(2_048);
            options
        }),
        context(),
        catalog,
        Uuid::from_u128(99),
    )
    .unwrap();

    assert_eq!(plan.table_oid.get(), 42);
    assert_eq!(plan.storage_id.as_str(), "00000007");
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
    assert_eq!(plan.backfill_job.payload["hot_row_limit"], 1000);
}

#[test]
fn existing_table_migration_plan_accepts_explicit_migration_order_by_from_options() {
    let catalog = ExistingTableCatalog::new(
        CatalogPrimaryKey::single(1, "id"),
        vec![
            CatalogColumn::uuid(1, "id").primary_key(),
            CatalogColumn::timestamp(2, "created_at"),
        ],
        vec![ColumnRef::new(ColumnId::from_attnum(2), "created_at")],
    );

    let plan = plan_existing_table_migration(
        &request(ManageTableOptions::from_value(&serde_json::json!({
            "migration_order_by": "created_at"
        }))),
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
    let catalog = ExistingTableCatalog::new(
        CatalogPrimaryKey::single(1, "id"),
        vec![CatalogColumn::uuid(1, "id").primary_key()],
        Vec::new(),
    );

    let error = plan_existing_table_migration(
        &request(ManageTableOptions::default()),
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
