use koldstore_common::SqlAccess as SpiAccess;
use koldstore_common::{
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
};
use koldstore_migrate::{
    backfill::plan_mirror_initialization_batch,
    jobs::{enqueue_migration_backfill_job_plan, MigrationBackfillJobRequest, MigrationBatchSize},
    order::{MigrationOrdering, OrderingSource},
    QualifiedTableName,
};
use uuid::Uuid;

fn table() -> QualifiedTableName {
    QualifiedTableName::parse("app.items").unwrap()
}

fn mirror() -> QualifiedTableName {
    QualifiedTableName::parse("koldstore.items__cl").unwrap()
}

fn pk_column(name: &str, ordinal: u16) -> PrimaryKeyColumnShape {
    PrimaryKeyColumnShape::new(
        PkColumn::new(name).unwrap(),
        PkOrdinal::new(ordinal).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("bigint").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    )
}

fn ordering() -> MigrationOrdering {
    MigrationOrdering {
        column: "id".to_string(),
        source: OrderingSource::AutoIncrementPrimaryKey,
        ascending_oldest_first: true,
    }
}

#[test]
fn populated_table_initialization_inserts_existing_primary_keys_without_touching_base_rows() {
    let plan = plan_mirror_initialization_batch(
        &table(),
        &mirror(),
        &[pk_column("id", 1)],
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
    assert!(plan.statement.sql.contains("mirror.\"id\" = hot.\"id\""));
    assert!(plan.statement.sql.contains("WHERE mirror.\"id\" IS NULL"));
    assert!(plan
        .statement
        .sql
        .contains("FOR KEY SHARE OF hot SKIP LOCKED"));
    assert!(plan
        .statement
        .sql
        .contains("ORDER BY hot.\"id\" ASC, hot.ctid ASC"));
    assert!(plan.statement.sql.contains("LIMIT $1"));
    assert!(plan
        .statement
        .sql
        .contains("INSERT INTO \"koldstore\".\"items__cl\""));
    assert!(plan.statement.sql.contains("(\"id\", \"seq\", \"op\")"));
    assert!(!plan.statement.sql.contains("commit_lsn"));
    assert!(plan
        .statement
        .sql
        .contains("SELECT \"id\", SNOWFLAKE_ID(), 1"));
    assert!(!plan.statement.sql.contains("pg_current_wal_lsn()"));
    assert!(plan
        .statement
        .sql
        .contains("SELECT count(*) FROM candidate"));
    assert!(plan
        .statement
        .sql
        .contains("SELECT count(*) FROM initialized"));

    for forbidden in [
        "UPDATE ONLY \"app\".\"items\"",
        "DELETE FROM \"app\".\"items\"",
        "\"_seq\"",
        "\"_commit_seq\"",
        "\"_deleted\"",
        "flush_seq_upper_bound",
    ] {
        assert!(
            !plan.statement.sql.contains(forbidden),
            "mirror initialization must not use legacy/base-row mutation fragment {forbidden}"
        );
    }
}

#[test]
fn populated_table_initialization_does_not_overwrite_newer_dml_state() {
    let plan = plan_mirror_initialization_batch(
        &table(),
        &mirror(),
        &[pk_column("tenant_id", 1), pk_column("id", 2)],
        MigrationOrdering {
            column: "created_at".to_string(),
            source: OrderingSource::ExplicitColumn,
            ascending_oldest_first: true,
        },
        MigrationBatchSize::new(500).unwrap(),
    )
    .unwrap();

    assert!(plan
        .statement
        .sql
        .contains("ON CONFLICT (\"tenant_id\", \"id\") DO NOTHING"));
    assert!(plan
        .statement
        .sql
        .contains("mirror.\"tenant_id\" = hot.\"tenant_id\" AND mirror.\"id\" = hot.\"id\""));
    assert!(plan
        .statement
        .sql
        .contains("WHERE mirror.\"tenant_id\" IS NULL"));
    assert!(plan
        .statement
        .sql
        .contains("SELECT \"tenant_id\", \"id\", SNOWFLAKE_ID(), 1"));
    assert!(plan
        .statement
        .sql
        .contains("hot.\"created_at\" AS migration_order_value"));
}

#[test]
fn mirror_initialization_job_starts_in_capturing_phase_not_system_column_phase() {
    let request = MigrationBackfillJobRequest::new(
        Uuid::from_u128(1),
        42,
        &table(),
        koldstore_migrate::jobs::ManagedTableType::Shared,
        Uuid::from_u128(2),
        None,
        &ordering(),
        MigrationBatchSize::new(10_000).unwrap(),
        Some(1_000),
    );
    let plan = enqueue_migration_backfill_job_plan(request).unwrap();

    assert_eq!(plan.payload["phase"], "initialize_mirror");
    assert!(plan.statement.sql.contains("'initialize_mirror'"));
    assert!(!plan.statement.sql.contains("'add_system_columns'"));
    assert!(!plan.statement.sql.contains("'backfill_seq'"));
}
