//! Built-in flush scheduler E2E: background worker ticks, auto_flush opt-out, interval GUC.
use crate::common;

use anyhow::Result;
use std::time::{Duration, Instant};

const SCHEDULER_DEADLINE: Duration = Duration::from_secs(30);
const NO_FLUSH_WINDOW: Duration = Duration::from_secs(4);

#[tokio::test]
async fn auto_flush_worker_flushes_without_manual_tick() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "sched_auto").await?;
        let relation = db.relation("msgs");
        let dbname = current_database(&db.client).await?;

        configure_flush_interval_and_restart_worker(&db.client, &dbname, 1).await?;
        create_messages_table(&db.client, &relation).await?;
        manage_auto_flush(&db.client, &relation, &db.storage_name, true).await?;
        common::wait_for_async_worker(&db.client).await?;

        insert_rows(&db.client, &relation, 1, 10).await?;
        let target_lsn = common::current_wal_lsn(&db.client).await?;
        common::wait_for_passive_convergence(
            &db.client,
            &relation,
            &target_lsn,
            1,
            5,
            SCHEDULER_DEADLINE,
        )
        .await?;
        reset_flush_interval(&db.client, &dbname).await?;
    }
    Ok(())
}

#[tokio::test]
async fn auto_flush_false_skips_scheduler_manual_flush_still_works() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "sched_off").await?;
        let relation = db.relation("msgs");
        let dbname = current_database(&db.client).await?;

        configure_flush_interval_and_restart_worker(&db.client, &dbname, 1).await?;
        create_messages_table(&db.client, &relation).await?;
        manage_auto_flush(&db.client, &relation, &db.storage_name, false).await?;

        insert_rows(&db.client, &relation, 1, 10).await?;
        // Even if a worker is running for other reasons, opt-out tables must not flush.
        let _ = common::terminate_async_worker(&db.client).await?;
        let _ = db
            .client
            .query_one(
                "SELECT koldstore.internal_ensure_async_mirror_worker()",
                &[],
            )
            .await?;
        tokio::time::sleep(NO_FLUSH_WINDOW).await;
        anyhow::ensure!(
            completed_flush_jobs(&db.client, &relation).await? == 0,
            "auto_flush=false must not background-flush"
        );

        let job_id = db
            .client
            .query_one(
                "SELECT koldstore.enqueue_flush_job(table_name => $1::text::regclass)::text",
                &[&relation],
            )
            .await?
            .get::<_, String>(0);
        anyhow::ensure!(!job_id.is_empty(), "enqueue must ignore auto_flush opt-out");

        // Enqueue publishes FLUSH_QUEUE_DIRTY; the supervisor may already be
        // running the one-shot executor. Wait on that job instead of racing a
        // second inline flush_table against the same table lock.
        let flushed = common::wait_for_flush_job_terminal(&db.client, &job_id).await?;
        anyhow::ensure!(
            flushed > 0,
            "manual enqueue must still flush when auto_flush=false"
        );
        anyhow::ensure!(completed_flush_jobs(&db.client, &relation).await? >= 1);

        reset_flush_interval(&db.client, &dbname).await?;
    }
    Ok(())
}

#[tokio::test]
async fn set_table_auto_flush_toggles_background_scheduling() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "sched_toggle").await?;
        let relation = db.relation("msgs");
        let dbname = current_database(&db.client).await?;

        configure_flush_interval_and_restart_worker(&db.client, &dbname, 1).await?;
        create_messages_table(&db.client, &relation).await?;
        manage_auto_flush(&db.client, &relation, &db.storage_name, false).await?;
        insert_rows(&db.client, &relation, 1, 10).await?;
        let target_lsn = common::current_wal_lsn(&db.client).await?;

        tokio::time::sleep(NO_FLUSH_WINDOW).await;
        anyhow::ensure!(completed_flush_jobs(&db.client, &relation).await? == 0);

        db.client
            .execute(
                "SELECT koldstore.set_table_auto_flush($1::text::regclass, true)",
                &[&relation],
            )
            .await?;
        common::wait_for_passive_convergence(
            &db.client,
            &relation,
            &target_lsn,
            1,
            5,
            SCHEDULER_DEADLINE,
        )
        .await?;

        db.client
            .execute(
                "SELECT koldstore.set_table_auto_flush($1::text::regclass, false)",
                &[&relation],
            )
            .await?;
        let auto_flush: bool = db
            .client
            .query_one(
                r#"
                SELECT COALESCE((options->>'auto_flush')::boolean, true)
                FROM koldstore.schemas
                WHERE table_oid = $1::text::regclass::oid AND active
                "#,
                &[&relation],
            )
            .await?
            .get(0);
        anyhow::ensure!(!auto_flush, "set_table_auto_flush(false) must persist");

        reset_flush_interval(&db.client, &dbname).await?;
    }
    Ok(())
}

#[tokio::test]
async fn flush_check_interval_seconds_is_honored_after_worker_restart() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "sched_interval").await?;
        let relation = db.relation("msgs");
        let dbname = current_database(&db.client).await?;

        create_messages_table(&db.client, &relation).await?;
        manage_auto_flush(&db.client, &relation, &db.storage_name, true).await?;

        // Drop interval to 1s and bounce the worker so it reloads database GUCs.
        // Also publish schedule-dirty so maintenance re-arms cadence from the new
        // interval instead of waiting out a prior default-30s deadline.
        configure_flush_interval_and_restart_worker(&db.client, &dbname, 1).await?;
        insert_rows(&db.client, &relation, 1, 10).await?;
        let target_lsn = common::current_wal_lsn(&db.client).await?;
        common::wait_for_passive_convergence(
            &db.client,
            &relation,
            &target_lsn,
            1,
            5,
            SCHEDULER_DEADLINE,
        )
        .await?;
        reset_flush_interval(&db.client, &dbname).await?;
    }
    Ok(())
}

/// After the database worker is (re)started, orphan `running` jobs are reclaimed
/// and auto-flush proceeds — the production path after postmaster / worker boot.
#[tokio::test]
async fn worker_startup_reclaims_orphan_running_and_auto_flushes() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "sched_boot").await?;
        let relation = db.relation("msgs");
        let dbname = current_database(&db.client).await?;

        configure_flush_interval_and_restart_worker(&db.client, &dbname, 1).await?;
        create_messages_table(&db.client, &relation).await?;
        manage_auto_flush(&db.client, &relation, &db.storage_name, true).await?;
        insert_rows(&db.client, &relation, 1, 10).await?;
        common::fence_async_mirror(&db.client).await?;

        // Pause dispatch before planting so WAL-side auto-enqueue cannot leave a
        // second active job (jobs_one_active_flush_per_scope_idx) or finish the
        // flush before reclaim runs.
        common::force_stop_async_worker(&db.client).await?;
        let completed_before = completed_flush_jobs(&db.client, &relation).await?;
        plant_orphan_running_flush(&db.client, &relation).await?;

        // Bounce the worker so its first flush-check tick reclaims + schedules.
        restart_database_worker(&db.client).await?;
        wait_for_completed_flush_jobs(
            &db.client,
            &relation,
            completed_before + 1,
            SCHEDULER_DEADLINE,
        )
        .await?;

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
                &[&relation],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            leftover_running == 0,
            "startup reclaim must clear orphan running jobs, got {leftover_running}"
        );

        reset_flush_interval(&db.client, &dbname).await?;
    }
    Ok(())
}

async fn current_database(client: &tokio_postgres::Client) -> Result<String> {
    Ok(client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0))
}

async fn set_flush_interval(
    client: &tokio_postgres::Client,
    dbname: &str,
    seconds: i32,
) -> Result<()> {
    client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.flush_check_interval_seconds = {seconds}"
        ))
        .await?;
    Ok(())
}

async fn reset_flush_interval(client: &tokio_postgres::Client, dbname: &str) -> Result<()> {
    let _ = client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_check_interval_seconds"
        ))
        .await;
    Ok(())
}

async fn configure_flush_interval_and_restart_worker(
    client: &tokio_postgres::Client,
    dbname: &str,
    seconds: i32,
) -> Result<()> {
    set_flush_interval(client, dbname, seconds).await?;
    let _ = common::terminate_async_worker(client).await?;
    // Ensure publishes schedule-dirty after commit so maintenance reloads the
    // database GUC and re-arms flush_check_interval cadence immediately.
    let _ = client
        .query_one(
            "SELECT koldstore.internal_ensure_async_mirror_worker()",
            &[],
        )
        .await?;
    Ok(())
}

/// Plants a crash-style `running` flush row with no session lock holder.
///
/// WAL apply may already have enqueued a pending/running job for the same
/// table. Convert that row in place when present; only INSERT when none exists
/// so we never trip `jobs_one_active_flush_per_scope_idx`.
async fn plant_orphan_running_flush(client: &tokio_postgres::Client, relation: &str) -> Result<()> {
    let updated = client
        .execute(
            r#"
            UPDATE koldstore.jobs
            SET status = 'running',
                phase = 'writing',
                attempt_token = NULL,
                available_at = clock_timestamp(),
                error_trace = NULL,
                payload = '{"force":false}'::jsonb,
                updated_at = clock_timestamp()
            WHERE table_oid = $1::text::regclass::oid
              AND scope_key = ''
              AND job_type = 'flush'
              AND status IN ('pending', 'running')
            "#,
            &[&relation],
        )
        .await?;
    if updated > 0 {
        return Ok(());
    }
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
        .await?;
    Ok(())
}

async fn restart_database_worker(client: &tokio_postgres::Client) -> Result<()> {
    let _ = common::terminate_async_worker(client).await?;
    // Clear backend ensure latch so the next ensure registers a fresh worker.
    tokio::time::sleep(Duration::from_millis(100)).await;
    common::wait_for_async_worker(client).await?;
    Ok(())
}

async fn create_messages_table(client: &tokio_postgres::Client, relation: &str) -> Result<()> {
    client
        .batch_execute(&format!(
            "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
        ))
        .await?;
    Ok(())
}

async fn manage_auto_flush(
    client: &tokio_postgres::Client,
    relation: &str,
    storage: &str,
    auto_flush: bool,
) -> Result<()> {
    // Keep max_rows_per_file small enough that a 10-row insert over hot_row_limit=5
    // is eligible (selected excess must be >= max_rows_per_file).
    client
        .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
        .await?;
    client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 5,
              min_flush_rows => 1,
              max_rows_per_file => 5,
              auto_flush => $3
            )
            "#,
            &[&relation, &storage, &auto_flush],
        )
        .await?;
    Ok(())
}

async fn insert_rows(
    client: &tokio_postgres::Client,
    relation: &str,
    from: i64,
    to: i64,
) -> Result<()> {
    client
        .execute(
            &format!(
                "INSERT INTO {relation} (id, body) \
                 SELECT id, 'b' || id FROM generate_series($1::bigint, $2::bigint) id"
            ),
            &[&from, &to],
        )
        .await?;
    Ok(())
}

async fn completed_flush_jobs(client: &tokio_postgres::Client, relation: &str) -> Result<i64> {
    Ok(client
        .query_one(
            r#"
            SELECT count(*)::bigint
            FROM koldstore.jobs
            WHERE table_oid = $1::text::regclass::oid
              AND job_type = 'flush'
              AND status = 'completed'
              AND created_at >= COALESCE(
                  (
                      SELECT created_at
                      FROM koldstore.schemas
                      WHERE table_oid = $1::text::regclass::oid AND active
                  ),
                  '-infinity'::timestamptz
              )
            "#,
            &[&relation],
        )
        .await?
        .get(0))
}

async fn wait_for_completed_flush_jobs(
    client: &tokio_postgres::Client,
    relation: &str,
    min_completed: i64,
    deadline: Duration,
) -> Result<()> {
    let started = Instant::now();
    loop {
        if completed_flush_jobs(client, relation).await? >= min_completed {
            return Ok(());
        }
        anyhow::ensure!(
            started.elapsed() <= deadline,
            "expected >= {min_completed} completed flush jobs for {relation} within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
