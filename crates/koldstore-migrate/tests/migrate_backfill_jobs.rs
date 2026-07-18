use koldstore_common::SqlAccess as SpiAccess;
use koldstore_common::{
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
};
use koldstore_migrate::{
    backfill::plan_mirror_initialization_batch,
    jobs::{
        enqueue_migration_backfill_job_plan, ManagedTableType, MigrationBackfillJobRequest,
        MigrationBatchSize,
    },
    order::{MigrationOrdering, OrderingSource},
    QualifiedTableName,
};
use uuid::Uuid;

fn table() -> QualifiedTableName {
    QualifiedTableName::parse("app.items").unwrap()
}

fn ordering() -> MigrationOrdering {
    MigrationOrdering {
        column: "id".to_string(),
        source: OrderingSource::AutoIncrementPrimaryKey,
        ascending_oldest_first: true,
    }
}
#[test]
fn existing_table_mirror_initialization_batches_without_rewriting_base_schema() {
    let pk = vec![PrimaryKeyColumnShape::new(
        PkColumn::new("id").unwrap(),
        PkOrdinal::new(1).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("bigint").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    )];
    let mirror = QualifiedTableName::parse("koldstore.items__cl").unwrap();
    let plan = plan_mirror_initialization_batch(
        &table(),
        &mirror,
        &pk,
        ordering(),
        MigrationBatchSize::new(1_000).unwrap(),
    )
    .unwrap();

    assert_eq!(plan.batch_size.get(), 1_000);
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan
        .statement
        .sql
        .contains("FROM ONLY \"app\".\"items\" AS hot"));
    assert!(plan
        .statement
        .sql
        .contains("LEFT JOIN \"koldstore\".\"items__cl\" AS mirror"));
    assert!(plan
        .statement
        .sql
        .contains("ON CONFLICT (\"id\") DO NOTHING"));
    assert!(plan.statement.sql.contains("\"op\""));
    assert!(plan.statement.sql.contains("1"));
    for forbidden in [
        "\"_seq\"",
        "\"_commit_seq\"",
        "\"_deleted\"",
        "\"_user_id\"",
    ] {
        assert!(!plan.statement.sql.contains(forbidden));
    }
}

#[test]
fn migration_backfill_job_payload_is_type_safe_and_operator_visible() {
    let job_id = Uuid::from_u128(11);
    let storage_id = Uuid::from_u128(22);
    let request = MigrationBackfillJobRequest::new(
        job_id,
        42,
        &table(),
        ManagedTableType::Shared,
        storage_id,
        None,
        &ordering(),
        MigrationBatchSize::new(10_000).unwrap(),
        Some(1_000),
    );

    let plan = enqueue_migration_backfill_job_plan(request).unwrap();

    assert_eq!(plan.job_id, job_id);
    assert_eq!(plan.table_oid, 42);
    assert_eq!(plan.payload["table_name"], "app.items");
    assert_eq!(plan.payload["table_type"], "shared");
    assert_eq!(plan.payload["order_column"], "id");
    assert_eq!(plan.payload["order_source"], "auto_increment_primary_key");
    assert_eq!(plan.payload["batch_size"], 10_000);
    assert_eq!(plan.payload["phase"], "initialize_mirror");
    assert_eq!(plan.payload["processed_rows"], 0);
    assert!(plan.statement.sql.contains("'migrate_backfill'"));
    assert!(plan.statement.sql.contains("'initialize_mirror'"));
    assert!(!plan.statement.sql.contains("'add_system_columns'"));
    assert!(plan.statement.sql.contains("rows_processed"));
    assert!(plan.statement.sql.contains("payload"));
    assert!(plan.statement.sql.contains("ON CONFLICT DO NOTHING"));
}
