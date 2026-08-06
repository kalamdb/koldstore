//! Production-default `flush_execution=queue` correctness (always-on; no SIGKILL).
//!
//! Most E2E fixtures force `inline` so session failpoints park Nested flush.
//! These tests override to queue and assert executor-backed completion, job
//! identity, orphan reclaim, and ordered reads after cold publish.

use crate::common;
use crate::flush::harness::wait_until_barrier_waiter;

use anyhow::{Context, Result};
use std::time::Duration;

/// Switches the fixture database to queue flush for new backends and this session.
async fn enable_queue_flush(db: &common::TestDb) -> Result<String> {
    let dbname: String = db
        .client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'queue'; \
             SET koldstore.flush_execution = 'queue';"
        ))
        .await
        .context("enable queue flush_execution")?;
    Ok(dbname)
}

async fn reset_flush_execution(db: &common::TestDb, dbname: &str) -> Result<()> {
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_execution; \
             RESET koldstore.flush_execution;"
        ))
        .await
        .ok();
    Ok(())
}

/// Queue-mode `flush_table` must spawn an executor, complete the job, and prune hot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_flush_table_completes_via_executor_and_prunes_hot() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "queue_flush_ok").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db.create_indexed_items_table("queue_items", 48).await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 8,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let job_id = common::flush_table_job_id(&db.client, &table.relation, true).await?;
        // Executor should appear briefly (may finish before we observe it under load).
        let _ = common::wait_for_flush_executor_pids(&db.client, Duration::from_secs(5)).await;
        let flushed = common::wait_for_flush_job_terminal(&db.client, &job_id).await?;
        anyhow::ensure!(flushed > 0, "queue flush archived no rows");
        common::wait_until_no_flush_executors(&db.client, Duration::from_secs(10)).await?;

        let hot = common::hot_row_count(&db.client, &table.relation).await?;
        anyhow::ensure!(
            hot <= 8,
            "queue flush must prune hot to hot_row_limit, hot={hot}"
        );
        let visible: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get(0);
        anyhow::ensure!(visible == 48, "managed count must stay 48, got {visible}");

        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}

/// Concurrent `ORDER BY … LIMIT` during a queue flush must not lose rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_limit_during_queue_flush_stays_complete() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "queue_ordered_lim").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db
            .create_indexed_items_table("queue_ord_items", 200)
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 20,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let relation = table.relation.clone();
        let reader = common::connect_peer(&db).await?;
        let reader_relation = relation.clone();
        let reader_handle = tokio::spawn(async move {
            let mut last_count = 0_i64;
            for _ in 0..40 {
                let count: i64 = reader
                    .query_one(
                        &format!("SELECT count(*) FROM {reader_relation} WHERE id >= 1"),
                        &[],
                    )
                    .await?
                    .get(0);
                let page: Vec<i64> = reader
                    .query(
                        &format!(
                            "SELECT id FROM {reader_relation} WHERE id >= 1 ORDER BY id LIMIT 25"
                        ),
                        &[],
                    )
                    .await?
                    .into_iter()
                    .map(|row| row.get(0))
                    .collect();
                anyhow::ensure!(
                    count == 200,
                    "visible count must stay 200 during queue flush, got {count}"
                );
                anyhow::ensure!(
                    page.len() == 25,
                    "ORDER BY LIMIT page must stay full, got {}",
                    page.len()
                );
                anyhow::ensure!(
                    page[0] == 1 && page[24] == 25,
                    "ordered page must be ids 1..=25, got {:?}",
                    page
                );
                last_count = count;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Ok::<_, anyhow::Error>(last_count)
        });

        let flushed = db.flush_table_with_force(&relation, true).await?;
        anyhow::ensure!(flushed > 0, "queue flush archived no rows");
        let final_count = reader_handle.await??;
        anyhow::ensure!(final_count == 200);

        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}

/// A durable `running` job with no owner must be reclaimed so queue flush proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_flush_reclaims_orphan_running_job_and_completes() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "queue_reclaim").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db
            .create_indexed_items_table("queue_reclaim_items", 32)
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 4,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        // Keep the background scheduler from reclaiming/spawning in parallel with
        // this test's explicit flush_table (same orphan row).
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.flush_check_interval_seconds = 3600;"
            ))
            .await?;

        db.client
            .execute(
                r#"
                INSERT INTO koldstore.jobs (
                  id, table_oid, scope_key, job_type, status, phase, payload
                ) VALUES (
                  gen_random_uuid(),
                  $1::text::regclass::oid,
                  '',
                  'flush',
                  'running',
                  'writing',
                  '{"force":false}'::jsonb
                )
                "#,
                &[&table.relation],
            )
            .await
            .context("insert orphan running flush job")?;

        let flushed = db.flush_table_with_force(&table.relation, true).await?;
        anyhow::ensure!(flushed > 0, "reclaim path must still flush rows");

        let leftover_running: i64 = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.jobs
                WHERE table_oid = $1::text::regclass::oid
                  AND job_type = 'flush'
                  AND status = 'running'
                "#,
                &[&table.relation],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            leftover_running == 0,
            "orphan running row must not remain, got {leftover_running}"
        );

        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_check_interval_seconds;"
            ))
            .await
            .ok();
        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}

/// Second queue flush while the first job is still active returns the same UUID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_dual_flush_returns_same_active_job() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "queue_dual").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db
            .create_indexed_items_table("queue_dual_items", 64)
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 8,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        // Park the executor so the first job stays active while we dual-call.
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.failpoint = 'wait:after_select_rows';"
            ))
            .await?;

        let coordinator = common::connect_peer(&db).await?;
        common::barrier_lock(&coordinator).await?;

        let first = common::flush_table_job_id(&db.client, &table.relation, true).await?;
        wait_until_barrier_waiter(&coordinator, || false)
            .await
            .context("queue executor should park at after_select_rows")?;

        let second = common::flush_table_job_id(&db.client, &table.relation, true).await?;
        anyhow::ensure!(
            first == second,
            "busy queue flush must return the active job id ({first} vs {second})"
        );

        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint; \
                 RESET koldstore.failpoint;"
            ))
            .await
            .ok();
        common::barrier_unlock(&coordinator).await?;
        let flushed = common::wait_for_flush_job_terminal(&db.client, &first).await?;
        anyhow::ensure!(flushed > 0);

        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}

/// Concurrent INSERT/UPDATE/SELECT while a queue flush executor runs must not lose rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_flush_with_concurrent_dml_keeps_visible_rows() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "queue_conc_dml").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db
            .create_indexed_items_table("queue_conc_items", 80)
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 10,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let relation = table.relation.clone();
        let writer = common::connect_peer(&db).await?;
        let writer_relation = relation.clone();
        let writer_handle = tokio::spawn(async move {
            for i in 0..20_i64 {
                let id = 1_000_i64 + i;
                writer
                    .execute(
                        &format!(
                            "INSERT INTO {writer_relation} (id, account_id, title, qty, category) \
                             VALUES ($1, 1, $2, 1, 'conc') \
                             ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty + 1"
                        ),
                        &[&id, &format!("conc-{id}")],
                    )
                    .await?;
                let count: i64 = writer
                    .query_one(
                        &format!("SELECT count(*) FROM {writer_relation} WHERE id >= 1"),
                        &[],
                    )
                    .await?
                    .get(0);
                anyhow::ensure!(
                    count >= 80,
                    "visible rows must not drop below seed during queue flush+DML, got {count}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        });

        let flushed = db.flush_table_with_force(&relation, true).await?;
        anyhow::ensure!(flushed > 0, "queue flush archived no rows");
        writer_handle.await??;

        let visible: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
            .await?
            .get(0);
        anyhow::ensure!(
            visible >= 80,
            "post concurrent queue flush visible count must stay >= 80, got {visible}"
        );

        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}

/// Multi-wave queue flush must keep ordered LIMIT correct across cold generations.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_multi_wave_ordered_limit_stays_correct() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "queue_multiwave").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db.create_indexed_items_table("queue_mw_items", 60).await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 8,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let first = db.flush_table_with_force(&table.relation, true).await?;
        anyhow::ensure!(first > 0);
        common::assert_no_active_jobs(&db.client, &table.relation).await?;

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, account_id, title, qty, category)
                SELECT
                  gs::bigint,
                  1,
                  'wave2-' || gs::text,
                  1,
                  'odd'
                FROM generate_series(61, 120) AS gs;
                "#,
                relation = table.relation
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let second = db.flush_table_with_force(&table.relation, true).await?;
        anyhow::ensure!(second > 0);

        let page: Vec<i64> = db
            .client
            .query(
                &format!(
                    "SELECT id FROM {} WHERE id >= $1 ORDER BY id LIMIT $2",
                    table.relation
                ),
                &[&1_i64, &10_i64],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        anyhow::ensure!(
            page == (1..=10).collect::<Vec<_>>(),
            "multi-wave ordered LIMIT must return 1..=10, got {page:?}"
        );
        let total: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get(0);
        anyhow::ensure!(
            total == 120,
            "multi-wave visible count must be 120, got {total}"
        );

        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}
