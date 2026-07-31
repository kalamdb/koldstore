use koldstore_common::{ColumnId, ColumnRef, ManageTableOptions};
use koldstore_migrate::{
    jobs::MigrationJobPhase,
    order::{CatalogColumn, CatalogPrimaryKey, OrderingSource},
    plan_existing_table_migration, ExistingTableCatalog, MigrateTableRequest,
    MigrationTableContext,
};
use uuid::Uuid;

fn request(options: ManageTableOptions) -> MigrateTableRequest {
    MigrateTableRequest {
        table_name: "app.items".to_string(),
        table_type: "shared".to_string(),
        storage_name: "local".to_string(),
        scope_column: None,
        options,
    }
}

fn context() -> MigrationTableContext {
    MigrationTableContext {
        table_oid: 42,
        storage_id: "00000007".to_string(),
    }
}

#[test]
fn existing_table_migration_plan_prepares_async_mirror_initialization_job() {
    let catalog = ExistingTableCatalog {
        primary_key: CatalogPrimaryKey::single(1, "id"),
        indexed_columns: vec![ColumnRef::new(ColumnId::from_attnum(2), "body")],
        columns: vec![
            CatalogColumn::bigint(1, "id")
                .primary_key()
                .default_expr("nextval('items_id_seq'::regclass)"),
            CatalogColumn::text(2, "body"),
        ],
    };

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

    assert_eq!(plan.table_oid, 42);
    assert_eq!(plan.storage_id, "00000007");
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
    let catalog = ExistingTableCatalog {
        primary_key: CatalogPrimaryKey::single(1, "id"),
        indexed_columns: vec![ColumnRef::new(ColumnId::from_attnum(2), "created_at")],
        columns: vec![
            CatalogColumn::uuid(1, "id").primary_key(),
            CatalogColumn::timestamp(2, "created_at"),
        ],
    };

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
    let catalog = ExistingTableCatalog {
        primary_key: CatalogPrimaryKey::single(1, "id"),
        indexed_columns: Vec::new(),
        columns: vec![CatalogColumn::uuid(1, "id").primary_key()],
    };

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
