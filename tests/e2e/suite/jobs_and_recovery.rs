use crate::common;

use anyhow::Result;

#[test]
fn jobs_and_recovery_contract_covers_status_retries_and_idempotence() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let recovery = koldstore_flush::ops::recover_segments_plan(
        Some(koldstore_common::TableName::parse("app.items").unwrap()),
        false,
    )
    .unwrap();

    assert!(recovery.statement.sql.contains("koldstore.jobs"));
    assert!(recovery.statement.sql.contains("recover_segments"));
    assert!(recovery.statement.sql.contains("attempts"));
    assert!(recovery.statement.sql.contains("dry_run"));
}

#[tokio::test]
async fn jobs_are_durable_idempotent_and_use_claim_indexes_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "jobs_and_recovery").await?;
        let table = db.create_indexed_items_table("job_items", 16).await?;
        db.manage_shared(&table.relation, "id").await?;

        let first_insert = db.insert_pending_flush_job(&table.relation).await?;
        let duplicate_insert = db.insert_pending_flush_job(&table.relation).await?;
        assert_eq!(first_insert, 1);
        assert_eq!(duplicate_insert, 0);
        assert_eq!(
            common::active_job_count(&db.client, &table.relation).await?,
            1
        );
        db.client.batch_execute("ANALYZE koldstore.jobs").await?;

        let pending_plan = common::explain_with_seqscan_disabled(
            &db.client,
            &format!(
                r#"
                SELECT id
                FROM koldstore.jobs
                WHERE table_oid = '{relation}'::regclass::oid
                  AND scope_key = ''
                  AND job_type = 'flush'
                  AND status IN ('pending', 'running')
                ORDER BY updated_at, id
                "#,
                relation = table.relation
            ),
        )
        .await?;
        common::assertions::assert_catalog_index_plan_uses_any(
            &pending_plan,
            &[
                "jobs_pending_idx",
                "jobs_claimable_by_type_idx",
                "jobs_claimable_idx",
                "jobs_one_active_flush_per_scope_idx",
            ],
        )?;

        let claim_plan = common::explain_with_seqscan_disabled(
            &db.client,
            r#"
            SELECT id
            FROM koldstore.jobs
            WHERE job_type = 'flush'
              AND status IN ('pending', 'running')
              AND run_after <= now()
              AND (status = 'pending' OR lease_expires_at IS NULL OR lease_expires_at < now())
            ORDER BY priority DESC, updated_at, id
            LIMIT 1
            "#,
        )
        .await?;
        common::assertions::assert_catalog_index_plan_uses_any(
            &claim_plan,
            &[
                "jobs_claimable_by_type_idx",
                "jobs_claimable_idx",
                "jobs_pending_idx",
                "jobs_one_active_flush_per_scope_idx",
                "jobs_one_active_table_work_idx",
            ],
        )?;

        let recovered = db
            .client
            .query_one(
                "SELECT koldstore.recover_segments($1::text::regclass, true)",
                &[&table.relation],
            )
            .await?;
        assert_eq!(recovered.get::<_, i64>(0), 0);
    }

    Ok(())
}

#[tokio::test]
async fn koldstore_runtime_gucs_are_registered_and_settable_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "runtime_gucs").await?;
        db.client
            .batch_execute(
                r#"
                SET koldstore.cold_reads = 'auto';
                SET koldstore.max_open_parquet_readers = 32;
                SET koldstore.max_running_jobs = 4;
                SET koldstore.log_level = 'info';
                "#,
            )
            .await?;

        let cold_reads = db
            .client
            .query_one("SHOW koldstore.cold_reads", &[])
            .await?
            .get::<_, String>(0);
        let max_readers = db
            .client
            .query_one("SHOW koldstore.max_open_parquet_readers", &[])
            .await?
            .get::<_, String>(0);
        let max_jobs = db
            .client
            .query_one("SHOW koldstore.max_running_jobs", &[])
            .await?
            .get::<_, String>(0);
        let log_level = db
            .client
            .query_one("SHOW koldstore.log_level", &[])
            .await?
            .get::<_, String>(0);

        assert_eq!(cold_reads, "auto");
        assert_eq!(max_readers, "32");
        assert_eq!(max_jobs, "4");
        assert_eq!(log_level, "info");
    }

    Ok(())
}

#[tokio::test]
async fn cold_reads_off_blocks_managed_scans_that_need_parquet_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cold_reads_off").await?;
        let table = db.create_indexed_items_table("cold_read_items", 16).await?;
        db.manage_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 16).await?;

        let plan = common::explain(
            &db.client,
            &format!("SELECT count(*) FROM {}", table.relation),
        )
        .await?;
        common::assert_kold_merge_scan_cold_reads(&plan, "manifest.json", 1)?;

        db.client
            .batch_execute(
                r#"
                SET koldstore.cold_reads = 'off';
                SET enable_seqscan = off;
                SET enable_indexscan = off;
                SET enable_bitmapscan = off;
                "#,
            )
            .await?;
        let blocked = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await;
        db.client
            .batch_execute(
                r#"
                SET koldstore.cold_reads = 'auto';
                RESET enable_seqscan;
                RESET enable_indexscan;
                RESET enable_bitmapscan;
                "#,
            )
            .await?;

        let error = blocked.expect_err("cold reads should fail closed");
        let message = error
            .as_db_error()
            .map(tokio_postgres::error::DbError::message)
            .unwrap_or_else(|| "missing database error");
        assert!(
            message.contains("cold reads are disabled by koldstore.cold_reads"),
            "unexpected cold-read error: {message}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn migrate_and_flush_sql_return_job_ids_and_expose_progress_on_pgrx() -> Result<()> {
    let mode = common::selected_mirror_capture_mode()?.as_str();
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_jobs_contract").await?;
        let table = db
            .create_indexed_items_table("async_contract_items", 8)
            .await?;

        let migrated = db
            .client
            .query_one(
                r#"
                WITH job AS (
                  SELECT koldstore.manage_table(
                      table_name     => $1::text::regclass,
                      storage        => $2,
                      hot_row_limit  => NULL,
                      migration_order_by => 'id',
                      mirror_capture_mode => $3
                    ) AS id
                )
                SELECT
                  pg_typeof(id)::text AS return_type,
                  id::text AS job_id
                FROM job
                "#,
                &[&table.relation, &db.storage_name, &mode],
            )
            .await?;
        assert_eq!(migrated.get::<_, String>("return_type"), "uuid");
        let migrate_job_id = migrated.get::<_, String>("job_id");

        let migrate_job = db
            .client
            .query_one(
                r#"
                SELECT status, phase, rows_processed
                FROM koldstore.jobs
                WHERE id = $1::text::uuid
                  AND table_oid = $2::text::regclass::oid
                  AND job_type = 'migrate_backfill'
                "#,
                &[&migrate_job_id, &table.relation],
            )
            .await?;
        assert!(["pending", "running", "completed"]
            .contains(&migrate_job.get::<_, String>("status").as_str()));

        let mirror_relation = format!("koldstore.{}__cl", table.table_name);
        wait_for_completed_job(&db.client, &migrate_job_id).await?;
        let base_rows = common::row_count(&db.client, &table.relation).await?;
        let mirror_rows = common::row_count(&db.client, &mirror_relation).await?;
        assert_eq!(mirror_rows, base_rows);

        let flushed = db
            .client
            .query_one(
                r#"
                WITH job AS (
                  SELECT koldstore.flush_table($1::text::regclass) AS id
                )
                SELECT
                  pg_typeof(id)::text AS return_type,
                  id::text AS job_id
                FROM job
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(flushed.get::<_, String>("return_type"), "uuid");
        let flush_job_id = flushed.get::<_, String>("job_id");

        let status_row = db
            .client
            .query_one(
                "SELECT koldstore.describe_table(table_name => $1::text::regclass)::text",
                &[&table.relation],
            )
            .await?;
        let status: serde_json::Value = serde_json::from_str(&status_row.get::<_, String>(0))?;
        assert!(
            status
                .get("jobs")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|jobs| {
                    jobs.iter().any(|job| {
                        job.get("id").and_then(serde_json::Value::as_str)
                            == Some(flush_job_id.as_str())
                            && job.get("status").is_some()
                            && job.get("phase").is_some()
                            && job.get("rows_flushed").is_some()
                    })
                }),
            "describe_table should expose job progress, got {status}"
        );

        wait_for_completed_job(&db.client, &flush_job_id).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, base_rows).await?;
    }

    Ok(())
}

#[tokio::test]
async fn extension_catalog_dml_is_blocked_but_storage_api_is_allowed_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "catalog_dml_blocked").await?;
        let app_role = format!("{}_app", db.schema);
        db.client
            .batch_execute(&format!(
                "DROP ROLE IF EXISTS {app_role}; CREATE ROLE {app_role};"
            ))
            .await?;

        db.client
            .batch_execute(&format!("SET ROLE {app_role};"))
            .await?;
        let direct_insert = db
            .client
            .execute(
                r#"
                INSERT INTO koldstore.storage (
                  id, name, storage_type, base_path, credentials, config
                )
                VALUES (
                  gen_random_uuid(), 'blocked', 'filesystem', '/tmp/blocked',
                  '{}'::jsonb, '{}'::jsonb
                )
                "#,
                &[],
            )
            .await;
        assert!(direct_insert.is_err());

        let api_insert = db
            .client
            .query_one(
                r#"
                SELECT koldstore.register_storage(
                  'api_allowed',
                  'filesystem',
                  '/tmp/api-allowed',
                  '{}'::jsonb,
                  '{}'::jsonb
                )
                "#,
                &[],
            )
            .await;
        db.client.batch_execute("RESET ROLE").await?;
        assert!(api_insert.is_ok());
    }

    Ok(())
}

async fn wait_for_completed_job(client: &tokio_postgres::Client, job_id: &str) -> Result<()> {
    for _ in 0..120 {
        let row = client
            .query_one(
                "SELECT status FROM koldstore.jobs WHERE id = $1::text::uuid",
                &[&job_id],
            )
            .await?;
        match row.get::<_, String>(0).as_str() {
            "completed" => return Ok(()),
            "error" => anyhow::bail!("job {job_id} failed"),
            _ => tokio::time::sleep(std::time::Duration::from_millis(250)).await,
        }
    }

    anyhow::bail!("job {job_id} did not complete")
}
