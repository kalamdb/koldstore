//! Production-default `flush_execution=queue` correctness (always-on; no SIGKILL).
//!
//! Most E2E fixtures force `inline` so session failpoints park Nested flush.
//! These tests override to queue and assert executor-backed completion, job
//! identity, orphan reclaim, and ordered reads after cold publish.

use crate::common;

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

/// Plants a durable `running` flush row with no session lock holder (crash orphan).
async fn plant_orphan_running_flush(client: &tokio_postgres::Client, relation: &str) -> Result<()> {
    client
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
            &[&relation],
        )
        .await
        .context("insert orphan running flush job")?;
    Ok(())
}

async fn running_flush_job_count(client: &tokio_postgres::Client, relation: &str) -> Result<i64> {
    Ok(client
        .query_one(
            r#"
            SELECT count(*)::bigint
            FROM koldstore.jobs
            WHERE table_oid = $1::text::regclass::oid
              AND job_type = 'flush'
              AND status = 'running'
            "#,
            &[&relation],
        )
        .await?
        .get(0))
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

        let job_id = common::flush_table_job_id(&db.client, &table.relation, true)
            .await?
            .context("force flush must return a job id")?;
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
        // Orphan payload starts as force=false; flush_table(..., true) must
        // reclaim + upgrade force so undersized policy cannot no-op the job.
        // ALTER DATABASE so the queue executor inherits the file-size floor.
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.min_max_rows_per_file = 1; \
                 SET koldstore.min_max_rows_per_file = 1;"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 4,
                  min_flush_rows => 1,
                  max_rows_per_file => 8,
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

        plant_orphan_running_flush(&db.client, &table.relation).await?;

        let flushed = db.flush_table_with_force(&table.relation, true).await?;
        anyhow::ensure!(
            flushed > 0,
            "reclaim path must still flush rows (got rows_flushed={flushed})"
        );

        let leftover_running = running_flush_job_count(&db.client, &table.relation).await?;
        anyhow::ensure!(
            leftover_running == 0,
            "orphan running row must not remain, got {leftover_running}"
        );

        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_check_interval_seconds; \
                 ALTER DATABASE \"{dbname}\" RESET koldstore.min_max_rows_per_file;"
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
    use crate::flush::harness::wait_until_barrier_waiter_deadline;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

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

        // Arm after manage/fence so only the flush executor inherits the wait.
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.failpoint = 'wait:after_select_rows';"
            ))
            .await?;

        let coordinator = common::connect_peer(&db).await?;
        common::barrier_lock(&coordinator).await?;

        let first = common::flush_table_job_id(&db.client, &table.relation, true)
            .await?
            .context("first force flush must return a job id")?;

        // Under parallel CI, spawn can lag; require a live executor before the
        // advisory wait row is expected.
        common::wait_for_flush_executor_pids(&db.client, Duration::from_secs(45))
            .await
            .context("queue dual-flush: wait for flush executor")?;

        let job_left_running = Arc::new(AtomicBool::new(false));
        let probe = common::connect_peer(&db).await?;
        let probe_job = first.clone();
        let left = Arc::clone(&job_left_running);
        let probe_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let status: Result<String, _> = probe
                    .query_one(
                        "SELECT status FROM koldstore.jobs WHERE id = $1::text::uuid",
                        &[&probe_job],
                    )
                    .await
                    .map(|row| row.get(0));
                match status {
                    Ok(status)
                        if matches!(status.as_str(), "completed" | "error" | "cancelled") =>
                    {
                        left.store(true, Ordering::SeqCst);
                        return;
                    }
                    Ok(_) => {}
                    Err(_) => {
                        left.store(true, Ordering::SeqCst);
                        return;
                    }
                }
            }
        });

        let wait_result = wait_until_barrier_waiter_deadline(
            &coordinator,
            || job_left_running.load(Ordering::SeqCst),
            Duration::from_secs(60),
        )
        .await;
        probe_handle.abort();
        if let Err(error) = wait_result {
            let status: String = db
                .client
                .query_one(
                    "SELECT status || ':' || coalesce(phase, '') || ':' || coalesce(error_trace, '') \
                     FROM koldstore.jobs WHERE id = $1::text::uuid",
                    &[&first],
                )
                .await
                .map(|row| row.get(0))
                .unwrap_or_else(|_| "<unreadable>".to_string());
            common::barrier_unlock(&coordinator).await.ok();
            let _ = db
                .client
                .batch_execute(&format!(
                    "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint; \
                     RESET koldstore.failpoint;"
                ))
                .await;
            return Err(error).context(format!(
                "queue executor should park at after_select_rows (job={first} state={status})"
            ));
        }

        let second = common::flush_table_job_id(&db.client, &table.relation, true)
            .await?
            .context("busy queue flush must return the active job id")?;
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

/// Concurrent force-flush callers must reclaim one orphan and all converge on
/// a completed job — the hang that used to leave `running` until wait budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_orphan_reclaim_survives_concurrent_force_flood() -> Result<()> {
    common::require_pgrx_server().await?;
    const CALLERS: usize = 8;
    const SEED_ROWS: i64 = 96;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "queue_orphan_flood").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db
            .create_indexed_items_table("queue_orphan_flood_items", SEED_ROWS)
            .await?;
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.min_max_rows_per_file = 1; \
                 SET koldstore.min_max_rows_per_file = 1; \
                 ALTER DATABASE \"{dbname}\" SET koldstore.flush_check_interval_seconds = 3600;"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 8,
                  min_flush_rows => 1,
                  max_rows_per_file => 16,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;
        plant_orphan_running_flush(&db.client, &table.relation).await?;

        let mut handles = Vec::with_capacity(CALLERS);
        for _ in 0..CALLERS {
            let peer = common::connect_peer(&db).await?;
            let relation = table.relation.clone();
            handles.push(tokio::spawn(async move {
                // Each peer must inherit queue mode from ALTER DATABASE.
                let job_id = common::flush_table_job_id(&peer, &relation, true)
                    .await?
                    .context("force flush under orphan must return a job id")?;
                let rows = common::wait_for_flush_job_terminal(&peer, &job_id).await?;
                Ok::<_, anyhow::Error>((job_id, rows))
            }));
        }

        let mut job_ids = std::collections::BTreeSet::new();
        let mut max_rows = 0_i64;
        for (idx, handle) in handles.into_iter().enumerate() {
            let (job_id, rows) = handle
                .await
                .with_context(|| format!("join concurrent orphan flush caller {idx}"))??;
            job_ids.insert(job_id);
            max_rows = max_rows.max(rows);
        }
        anyhow::ensure!(
            job_ids.len() == 1,
            "concurrent reclaim must converge on one durable job UUID, got {job_ids:?}"
        );
        anyhow::ensure!(
            max_rows > 0,
            "reclaimed orphan force flush must archive rows"
        );
        anyhow::ensure!(
            running_flush_job_count(&db.client, &table.relation).await? == 0,
            "no running flush jobs may remain after concurrent reclaim flood"
        );

        let visible: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get(0);
        anyhow::ensure!(
            visible == SEED_ROWS,
            "visible count must stay {SEED_ROWS} after reclaim flood, got {visible}"
        );
        let hot = common::hot_row_count(&db.client, &table.relation).await?;
        anyhow::ensure!(
            hot <= 8,
            "hot must be at or under limit after flood, hot={hot}"
        );

        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_check_interval_seconds; \
                 ALTER DATABASE \"{dbname}\" RESET koldstore.min_max_rows_per_file;"
            ))
            .await
            .ok();
        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}

/// Repeated plant-orphan → force-flush waves with interleaved DML must never
/// leave a stuck `running` job or drop visible rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_orphan_reclaim_across_repeated_waves_with_dml() -> Result<()> {
    common::require_pgrx_server().await?;
    const WAVES: i64 = 6;
    const ROWS_PER_WAVE: i64 = 24;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "queue_orphan_waves").await?;
        let dbname = enable_queue_flush(&db).await?;
        let table = db
            .create_indexed_items_table("queue_orphan_wave_items", ROWS_PER_WAVE)
            .await?;
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.min_max_rows_per_file = 1; \
                 SET koldstore.min_max_rows_per_file = 1; \
                 ALTER DATABASE \"{dbname}\" SET koldstore.flush_check_interval_seconds = 3600;"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 6,
                  min_flush_rows => 1,
                  max_rows_per_file => 12,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let mut expected = ROWS_PER_WAVE;
        for wave in 1..=WAVES {
            plant_orphan_running_flush(&db.client, &table.relation).await?;

            // Interleave writers while reclaim/flush runs so WAL apply + queue
            // reclaim overlap the way production load does.
            let writer = common::connect_peer(&db).await?;
            let relation = table.relation.clone();
            let start_id = expected + 1;
            let end_id = expected + ROWS_PER_WAVE;
            let writer_handle = tokio::spawn(async move {
                writer
                    .execute(
                        &format!(
                            "INSERT INTO {relation} (id, account_id, title, qty, category) \
                             SELECT gs, gs % 17, 'item-' || lpad(gs::text, 6, '0'), \
                                    (gs % 100)::integer, \
                                    CASE WHEN gs % 2 = 0 THEN 'even' ELSE 'odd' END \
                             FROM generate_series($1::bigint, $2::bigint) AS gs"
                        ),
                        &[&start_id, &end_id],
                    )
                    .await?;
                Ok::<_, anyhow::Error>(())
            });

            let mut flushers = Vec::with_capacity(4);
            for _ in 0..4 {
                let peer = common::connect_peer(&db).await?;
                let relation = table.relation.clone();
                flushers.push(tokio::spawn(async move {
                    let job_id = common::flush_table_job_id(&peer, &relation, true)
                        .await?
                        .context("wave force flush must return job id")?;
                    common::wait_for_flush_job_terminal(&peer, &job_id).await
                }));
            }

            writer_handle.await??;
            expected = end_id;
            common::fence_async_mirror(&db.client).await?;

            let mut any_rows = 0_i64;
            for (idx, handle) in flushers.into_iter().enumerate() {
                let rows = handle
                    .await
                    .with_context(|| format!("join wave {wave} flusher {idx}"))??;
                any_rows = any_rows.max(rows);
            }
            anyhow::ensure!(
                any_rows > 0,
                "wave {wave}: reclaim force flush must archive rows"
            );
            anyhow::ensure!(
                running_flush_job_count(&db.client, &table.relation).await? == 0,
                "wave {wave}: leftover running flush job after reclaim"
            );

            let visible: i64 = db
                .client
                .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
                .await?
                .get(0);
            anyhow::ensure!(
                visible == expected,
                "wave {wave}: visible count must be {expected}, got {visible}"
            );
        }

        // Final catch-up flush after the last wave's inserts.
        let _ = db.flush_table_with_force(&table.relation, true).await?;
        anyhow::ensure!(running_flush_job_count(&db.client, &table.relation).await? == 0);
        let hot = common::hot_row_count(&db.client, &table.relation).await?;
        anyhow::ensure!(hot <= 6, "final hot must be <= limit, hot={hot}");

        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_check_interval_seconds; \
                 ALTER DATABASE \"{dbname}\" RESET koldstore.min_max_rows_per_file;"
            ))
            .await
            .ok();
        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}

/// Orphans on multiple tables reclaimed under parallel force-flush must all
/// complete without cross-table stuck `running` rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_multi_table_orphan_reclaim_under_parallel_flush() -> Result<()> {
    common::require_pgrx_server().await?;
    const TABLES: &[&str] = &["orphan_a", "orphan_b", "orphan_c", "orphan_d"];
    const SEED_ROWS: i64 = 48;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "queue_multi_orphan").await?;
        let dbname = enable_queue_flush(&db).await?;
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.min_max_rows_per_file = 1; \
                 SET koldstore.min_max_rows_per_file = 1; \
                 ALTER DATABASE \"{dbname}\" SET koldstore.flush_check_interval_seconds = 3600;"
            ))
            .await?;

        let mut relations = Vec::new();
        for name in TABLES {
            let table = db.create_indexed_items_table(name, SEED_ROWS).await?;
            db.client
                .execute(
                    r#"
                    SELECT koldstore.manage_table(
                      table_name => $1::text::regclass,
                      storage => $2,
                      hot_row_limit => 6,
                      min_flush_rows => 1,
                      max_rows_per_file => 12,
                      migration_order_by => 'id',
                      auto_flush => false
                    )
                    "#,
                    &[&table.relation, &db.storage_name],
                )
                .await?;
            plant_orphan_running_flush(&db.client, &table.relation).await?;
            relations.push(table.relation);
        }
        common::fence_async_mirror(&db.client).await?;

        let mut handles = Vec::new();
        for relation in &relations {
            // Two concurrent reclaim callers per table.
            for _ in 0..2 {
                let peer = common::connect_peer(&db).await?;
                let relation = relation.clone();
                handles.push(tokio::spawn(async move {
                    let job_id = common::flush_table_job_id(&peer, &relation, true)
                        .await?
                        .context("multi-table orphan force flush must return job id")?;
                    let rows = common::wait_for_flush_job_terminal(&peer, &job_id).await?;
                    Ok::<_, anyhow::Error>((relation, job_id, rows))
                }));
            }
        }

        let mut per_table: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
            std::collections::BTreeMap::new();
        for (idx, handle) in handles.into_iter().enumerate() {
            let (relation, job_id, rows) = handle
                .await
                .with_context(|| format!("join multi-table orphan flusher {idx}"))??;
            anyhow::ensure!(rows > 0, "{relation}: reclaim flush archived no rows");
            per_table.entry(relation).or_default().insert(job_id);
        }
        for relation in &relations {
            let ids = per_table
                .get(relation)
                .context(format!("missing results for {relation}"))?;
            anyhow::ensure!(
                ids.len() == 1,
                "{relation}: concurrent reclaim must share one job UUID, got {ids:?}"
            );
            anyhow::ensure!(
                running_flush_job_count(&db.client, relation).await? == 0,
                "{relation}: leftover running after parallel multi-table reclaim"
            );
            let visible: i64 = db
                .client
                .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
                .await?
                .get(0);
            anyhow::ensure!(
                visible == SEED_ROWS,
                "{relation}: visible count must stay {SEED_ROWS}, got {visible}"
            );
        }

        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_check_interval_seconds; \
                 ALTER DATABASE \"{dbname}\" RESET koldstore.min_max_rows_per_file;"
            ))
            .await
            .ok();
        reset_flush_execution(&db, &dbname).await?;
    }
    Ok(())
}
