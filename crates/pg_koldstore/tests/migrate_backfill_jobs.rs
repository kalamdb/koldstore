use koldstore_common::{
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
};
use koldstore_migrate::{
    backfill::plan_mirror_initialization_batch,
    jobs::{
        claim_migration_jobs_plan, enqueue_migration_backfill_job_plan,
        finish_mirror_initialization_plan, migration_job_progress_plan, ManagedTableType,
        MigrationBackfillJobRequest, MigrationBatchSize, MigrationJobPhase, MigrationLeaseEpoch,
        MigrationLeaseSeconds,
    },
    order::{MigrationOrdering, OrderingSource},
    QualifiedTableName,
};
use pg_koldstore::spi::SpiAccess;
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

#[test]
fn migration_job_claims_are_lease_guarded_and_skip_locked() {
    let plan = claim_migration_jobs_plan(64, 4, MigrationLeaseSeconds::new(45).unwrap()).unwrap();

    assert_eq!(plan.limit, 64);
    assert_eq!(plan.max_running_jobs, 4);
    assert_eq!(plan.lease_seconds.get(), 45);
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan
        .statement
        .sql
        .contains("job_type IN ('migrate_backfill')"));
    assert!(plan.statement.sql.contains("FOR UPDATE SKIP LOCKED"));
    assert!(plan.statement.sql.contains("$4::integer"));
    assert!(plan.statement.sql.contains("running_jobs"));
    assert!(plan.statement.sql.contains("table_running"));
    assert!(plan
        .statement
        .sql
        .contains("lease_epoch = j.lease_epoch + 1"));
    assert!(plan
        .statement
        .sql
        .contains("THEN 'initialize_mirror' ELSE j.phase END"));
    assert!(plan.statement.sql.contains("rows_processed"));
    assert!(plan.statement.sql.contains("last_heartbeat_at = now()"));
}

#[test]
fn migration_progress_updates_are_guarded_by_the_live_lease() {
    let job_id = Uuid::from_u128(31);
    let owner = Uuid::from_u128(32);
    let plan = migration_job_progress_plan(
        job_id,
        owner,
        MigrationLeaseEpoch::new(4).unwrap(),
        MigrationJobPhase::InitializeMirror,
        512,
    )
    .unwrap();

    assert_eq!(plan.job_id, job_id);
    assert_eq!(plan.lease_owner, owner);
    assert_eq!(plan.lease_epoch.get(), 4);
    assert_eq!(plan.phase, MigrationJobPhase::InitializeMirror);
    assert_eq!(plan.rows_processed_increment, 512);
    assert!(plan.statement.sql.contains("lease_owner = $2::uuid"));
    assert!(plan.statement.sql.contains("lease_epoch = $3::bigint"));
    assert!(plan
        .statement
        .sql
        .contains("rows_processed = rows_processed + $5::bigint"));
    assert!(!plan.statement.sql.contains("checkpoint_seq ="));
}

#[test]
fn finishing_mirror_initialization_activates_schema_without_initial_flush() {
    let plan = finish_mirror_initialization_plan(
        Uuid::from_u128(41),
        Uuid::from_u128(42),
        MigrationLeaseEpoch::new(5).unwrap(),
    )
    .unwrap();

    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan.statement.sql.contains("status = 'completed'"));
    assert!(plan.statement.sql.contains("phase = 'finished'"));
    assert!(plan.statement.sql.contains("UPDATE koldstore.schemas AS s"));
    assert!(plan.statement.sql.contains("SET active = true"));
    assert!(plan
        .statement
        .sql
        .contains("initialization_state = 'complete'"));
    assert!(!plan.statement.sql.contains("INSERT INTO koldstore.jobs"));
    assert!(!plan.statement.sql.contains("flush_seq_upper_bound"));
}

#[test]
fn catalog_schema_supports_migration_job_status_and_concurrency() {
    let sql = include_str!("../sql/koldstore--0.1.0.sql");

    assert!(sql.contains("rows_processed bigint NOT NULL DEFAULT 0"));
    assert!(sql.contains("jobs_claimable_by_type_idx"));
    assert!(sql.contains("ON koldstore.jobs (job_type, status, run_after"));
    assert!(sql.contains("jobs_one_active_table_work_idx"));
    assert!(sql.contains("jobs_one_active_migration_per_table_idx"));
    assert!(
        sql.contains("WHERE job_type IN ('migrate_backfill') AND status IN ('pending', 'running')")
    );
}
