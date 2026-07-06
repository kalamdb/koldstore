use koldstore_core::SeqId;
use pg_koldstore::{
    migrate::{
        columns::{
            plan_existing_table_system_column_finalize, plan_existing_table_system_column_prepare,
        },
        jobs::{
            backfill_batch_plan, claim_migration_jobs_plan, enqueue_migration_backfill_job_plan,
            finish_backfill_and_enqueue_flush_plan, migration_job_progress_plan, ManagedTableType,
            MigrationBackfillJobRequest, MigrationBatchSize, MigrationJobPhase,
            MigrationLeaseEpoch, MigrationLeaseSeconds,
        },
        order::{MigrationOrdering, OrderingSource},
        QualifiedTableName,
    },
    spi::SpiAccess,
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
fn existing_table_system_columns_are_prepared_without_rewriting_old_rows() {
    let prepare = plan_existing_table_system_column_prepare(&table(), false).unwrap();
    let finalize = plan_existing_table_system_column_finalize(&table()).unwrap();

    assert_eq!(prepare.columns, vec!["_seq", "_commit_seq", "_deleted"]);
    assert_eq!(prepare.statements.len(), 2);
    assert!(prepare
        .statements
        .iter()
        .all(|statement| statement.access == SpiAccess::ReadWrite));
    assert!(prepare.statements[0]
        .sql
        .contains("ADD COLUMN IF NOT EXISTS \"_seq\" bigint"));
    assert!(!prepare.statements[0].sql.contains("NOT NULL"));
    assert!(!prepare.statements[0].sql.contains("DEFAULT"));

    assert!(prepare.statements[1]
        .sql
        .contains("ALTER COLUMN \"_seq\" SET DEFAULT SNOWFLAKE_ID()"));
    assert!(prepare.statements[1]
        .sql
        .contains("ALTER COLUMN \"_deleted\" SET DEFAULT false"));
    assert!(finalize
        .statement
        .sql
        .contains("ALTER COLUMN \"_seq\" SET NOT NULL"));
}

#[test]
fn backfill_batch_plan_is_bounded_ordered_and_uses_skip_locked_rows() {
    let plan = backfill_batch_plan(
        &table(),
        ordering(),
        MigrationBatchSize::new(1_000).unwrap(),
    )
    .unwrap();

    assert_eq!(plan.batch_size.get(), 1_000);
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan.statement.sql.contains("FROM ONLY \"app\".\"items\""));
    assert!(plan.statement.sql.contains("WHERE \"_seq\" IS NULL"));
    assert!(plan.statement.sql.contains("ORDER BY \"id\" ASC, ctid ASC"));
    assert!(plan.statement.sql.contains("LIMIT $1"));
    assert!(plan.statement.sql.contains("FOR UPDATE SKIP LOCKED"));
    assert!(plan
        .statement
        .sql
        .contains("nextval('koldstore.global_seq'::regclass) AS assigned_seq"));
    assert!(plan.statement.sql.contains("AND hot.\"_seq\" IS NULL"));
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
        Some("rows:1000,interval:60".to_string()),
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
    let plan = claim_migration_jobs_plan(64, MigrationLeaseSeconds::new(45).unwrap()).unwrap();

    assert_eq!(plan.limit, 64);
    assert_eq!(plan.lease_seconds.get(), 45);
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan
        .statement
        .sql
        .contains("job_type IN ('migrate_backfill')"));
    assert!(plan.statement.sql.contains("FOR UPDATE SKIP LOCKED"));
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
        MigrationJobPhase::BackfillSeq,
        SeqId::new(999).unwrap(),
        512,
    )
    .unwrap();

    assert_eq!(plan.job_id, job_id);
    assert_eq!(plan.lease_owner, owner);
    assert_eq!(plan.lease_epoch.get(), 4);
    assert_eq!(plan.phase, MigrationJobPhase::BackfillSeq);
    assert_eq!(plan.rows_processed_increment, 512);
    assert!(plan.statement.sql.contains("lease_owner = $2::uuid"));
    assert!(plan.statement.sql.contains("lease_epoch = $3::bigint"));
    assert!(plan
        .statement
        .sql
        .contains("checkpoint_seq = GREATEST(checkpoint_seq, $5::bigint)"));
    assert!(plan
        .statement
        .sql
        .contains("rows_processed = rows_processed + $6::bigint"));
}

#[test]
fn finishing_backfill_activates_schema_and_enqueues_watermarked_flush() {
    let plan = finish_backfill_and_enqueue_flush_plan(
        &table(),
        None,
        Uuid::from_u128(41),
        Uuid::from_u128(42),
        MigrationLeaseEpoch::new(5).unwrap(),
        SeqId::new(10_000).unwrap(),
    )
    .unwrap();

    assert_eq!(plan.flush_seq_upper_bound.get(), 10_000);
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan.statement.sql.contains("status = 'completed'"));
    assert!(plan.statement.sql.contains("phase = 'finished'"));
    assert!(plan
        .statement
        .sql
        .contains("flush_seq_upper_bound = $4::bigint"));
    assert!(plan.statement.sql.contains("UPDATE koldstore.schemas AS s"));
    assert!(plan.statement.sql.contains("SET active = true"));
    assert!(plan.statement.sql.contains("INSERT INTO koldstore.jobs"));
    assert!(plan.statement.sql.contains("'flush'"));
    assert!(plan
        .statement
        .sql
        .contains("jsonb_build_object('source', 'migration', 'force', false)"));
    assert!(plan.statement.sql.contains("SELECT ''::text AS scope_key"));
}

#[test]
fn finishing_user_backfill_enqueues_one_watermarked_flush_per_scope() {
    let plan = finish_backfill_and_enqueue_flush_plan(
        &table(),
        Some("tenant_id"),
        Uuid::from_u128(51),
        Uuid::from_u128(52),
        MigrationLeaseEpoch::new(6).unwrap(),
        SeqId::new(20_000).unwrap(),
    )
    .unwrap();

    assert_eq!(plan.scope_column.as_deref(), Some("tenant_id"));
    assert!(plan
        .statement
        .sql
        .contains("SELECT DISTINCT COALESCE(hot.\"tenant_id\"::text, '') AS scope_key"));
    assert!(plan
        .statement
        .sql
        .contains("FROM ONLY \"app\".\"items\" AS hot"));
    assert!(plan
        .statement
        .sql
        .contains("WHERE hot.\"_seq\" <= $4::bigint"));
}

#[test]
fn catalog_schema_supports_migration_job_status_and_concurrency() {
    let sql = include_str!("../sql/koldstore--0.1.0.sql");

    assert!(sql.contains("rows_processed bigint NOT NULL DEFAULT 0"));
    assert!(sql.contains("jobs_claimable_by_type_idx"));
    assert!(sql.contains("ON koldstore.jobs (job_type, status, run_after"));
    assert!(sql.contains("jobs_one_active_migration_per_table_idx"));
    assert!(
        sql.contains("WHERE job_type IN ('migrate_backfill') AND status IN ('pending', 'running')")
    );
}
