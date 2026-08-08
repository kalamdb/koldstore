//! Long multi-segment flush + SIGKILL executor, then full data-plane recovery.
//!
//! Mimics crashing PostgreSQL while a long flush is in flight: the queue
//! executor parks after writing a temp Parquet object, SIGKILL triggers
//! postmaster crash recovery, then `recover_segments` + force retry must leave
//! hot/mirror/query/cold Parquet/manifest consistent.
//!
//! Gated by `KOLDSTORE_CRASH_FLUSH_EXECUTOR=1` (same as [`super::flush_executor_kill`]).

use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::time::sleep;

use crate::common;
use crate::crash::invariants::{assert_recovered_flush_data_plane, RecoveredFlushExpect};
use crate::flush::harness::{barrier_lock, connect_peer, wait_until_barrier_waiter};

/// Seed rows large enough for multiple `max_rows_per_file` segments.
const LONG_FLUSH_ROWS: i64 = 120;
/// Keep hot tiny so excess spans several Parquet files.
const HOT_ROW_LIMIT: i64 = 8;
const MAX_ROWS_PER_FILE: i64 = 16;

fn flush_executor_kill_enabled() -> bool {
    matches!(
        std::env::var("KOLDSTORE_CRASH_FLUSH_EXECUTOR")
            .ok()
            .as_deref(),
        Some("1") | Some("true")
    )
}

/// Crash mid multi-segment flush and verify mirror, hot, queries, and cold objects.
#[tokio::test]
async fn long_flush_executor_sigkill_recovers_mirror_hot_query_and_cold() -> Result<()> {
    if !flush_executor_kill_enabled() {
        eprintln!(
            "skipping long flush executor SIGKILL data-plane test \
             (set KOLDSTORE_CRASH_FLUSH_EXECUTOR=1)"
        );
        return Ok(());
    }

    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        run_long_flush_kill(target).await?;
    }
    Ok(())
}

async fn run_long_flush_kill(target: common::PgTarget) -> Result<()> {
    let db = common::TestDb::start(target, "crash_long_flush_kill").await?;
    let reopen_target = db.target.clone();
    let table = db
        .create_indexed_items_table("long_flush_kill_items", LONG_FLUSH_ROWS)
        .await?;
    let relation = table.relation.clone();
    let reference = db.relation("long_flush_kill_ref");
    common::create_reference_clone(&db.client, &relation, &reference, &["id"]).await?;

    let dbname: String = db
        .client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);

    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'queue'; \
             ALTER DATABASE \"{dbname}\" SET koldstore.min_max_rows_per_file = 1; \
             SET koldstore.flush_execution = 'queue'; \
             SET koldstore.min_max_rows_per_file = 1;"
        ))
        .await?;

    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => $3,
              min_flush_rows => 1,
              max_rows_per_file => $4,
              migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[
                &relation,
                &db.storage_name,
                &HOT_ROW_LIMIT,
                &MAX_ROWS_PER_FILE,
            ],
        )
        .await
        .context("manage_table")?;

    common::fence_async_mirror(&db.client).await?;

    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.failpoint = 'wait:after_temp_object';"
        ))
        .await
        .context("arm database failpoint")?;

    let coordinator = connect_peer(&db).await?;
    barrier_lock(&coordinator).await?;

    let job_id: String = db
        .client
        .query_one(
            "SELECT (koldstore.flush_table($1::text::regclass, true)->>'job_id')",
            &[&relation],
        )
        .await
        .context("enqueue long flush_table")?
        .get(0);

    let pids = common::wait_for_flush_executor_pids(&db.client, Duration::from_secs(45))
        .await
        .context("wait for flush executor")?;
    wait_until_barrier_waiter(&coordinator, || false)
        .await
        .context("wait for executor failpoint barrier")?;

    for pid in &pids {
        common::sigkill_pid(*pid).with_context(|| format!("SIGKILL flush executor pid={pid}"))?;
    }

    let client = common::wait_for_postgres(&reopen_target)
        .await
        .context("wait for postgres after long-flush SIGKILL")?;
    client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.failpoint = ''; \
             ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'inline'; \
             SET koldstore.failpoint = ''; \
             SET koldstore.flush_execution = 'inline'; \
             SELECT pg_advisory_unlock_all();"
        ))
        .await
        .context("disarm failpoint / switch retry to inline")?;

    let _ = client
        .query_one(
            "SELECT koldstore.recover_segments($1::text::regclass, false)",
            &[&relation],
        )
        .await
        .context("recover_segments after long-flush kill")?;

    // Force drain until hot is empty so prune asserts apply.
    let mut passes = 0;
    loop {
        passes += 1;
        anyhow::ensure!(
            passes <= 16,
            "force flush did not drain after {passes} passes"
        );
        let retry_job: Option<String> = client
            .query_one(
                "SELECT (koldstore.flush_table($1::text::regclass, true)->>'job_id')",
                &[&relation],
            )
            .await
            .context("retry flush_table after kill")?
            .get(0);
        let Some(retry_job) = retry_job.filter(|value| !value.is_empty() && value != "null") else {
            break;
        };
        wait_for_job_terminal(&client, &retry_job, Duration::from_secs(120)).await?;
        let hot = common::hot_row_count(&client, &relation).await?;
        if hot == 0 {
            break;
        }
    }

    wait_for_job_not_stuck_running(&client, &job_id).await?;

    let min_segments = (LONG_FLUSH_ROWS / MAX_ROWS_PER_FILE).max(2);
    let compare_cols = ["id", "account_id", "title", "qty", "category"];
    assert_recovered_flush_data_plane(
        &client,
        &relation,
        &db.storage_root,
        RecoveredFlushExpect {
            visible_rows: LONG_FLUSH_ROWS,
            expect_hot_fully_pruned: true,
            min_cold_segments: min_segments,
            reference: Some((reference.as_str(), &compare_cols)),
        },
    )
    .await
    .context("post long-flush kill data-plane checks")?;

    Ok(())
}

async fn wait_for_job_terminal(
    client: &tokio_postgres::Client,
    job_id: &str,
    deadline: Duration,
) -> Result<()> {
    let started = std::time::Instant::now();
    loop {
        let status: String = client
            .query_one(
                "SELECT status FROM koldstore.jobs WHERE id = $1::text::uuid",
                &[&job_id],
            )
            .await?
            .get(0);
        match status.as_str() {
            "completed" | "cancelled" => return Ok(()),
            "error" => bail!("job {job_id} ended in error after long-flush kill recovery"),
            _ => {
                if started.elapsed() > deadline {
                    bail!("job {job_id} still {status} after {deadline:?}");
                }
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn wait_for_job_not_stuck_running(
    client: &tokio_postgres::Client,
    job_id: &str,
) -> Result<()> {
    let status: String = client
        .query_one(
            "SELECT status FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        )
        .await?
        .get(0);
    if status == "running" {
        sleep(Duration::from_millis(500)).await;
        let status: String = client
            .query_one(
                "SELECT status FROM koldstore.jobs WHERE id = $1::text::uuid",
                &[&job_id],
            )
            .await?
            .get(0);
        if status == "running" {
            bail!("job {job_id} stuck in running after long-flush SIGKILL");
        }
    }
    Ok(())
}
