//! SIGKILL a queue-mode flush executor mid-flush (triggers PG crash recovery).
//!
//! Gated by `KOLDSTORE_CRASH_FLUSH_EXECUTOR=1` (destructive; not CI-default).
//!
//! PostgreSQL treats an abnormal bgworker death (including external SIGKILL) as
//! a backend crash and restarts the cluster. That is the intended coverage:
//! mid-flush process death → auto recovery → `recover_segments` + retry flush
//! with full data-plane checks.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::time::sleep;

use crate::common;
use crate::crash::invariants::{assert_recovered_flush_data_plane, RecoveredFlushExpect};
use crate::flush::harness::{barrier_lock, connect_peer, wait_until_barrier_waiter};

fn flush_executor_kill_enabled() -> bool {
    matches!(
        std::env::var("KOLDSTORE_CRASH_FLUSH_EXECUTOR")
            .ok()
            .as_deref(),
        Some("1") | Some("true")
    )
}

#[tokio::test]
async fn flush_executor_sigkill_recovers_and_completes() -> Result<()> {
    if !flush_executor_kill_enabled() {
        eprintln!(
            "skipping flush executor SIGKILL crash test \
             (set KOLDSTORE_CRASH_FLUSH_EXECUTOR=1)"
        );
        return Ok(());
    }

    // SIGKILL of a bgworker restarts the whole postmaster — hold the cluster
    // exclusive so sibling E2E fixtures are not mid-query when connections die.
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        run_executor_kill(target).await?;
    }
    Ok(())
}

async fn run_executor_kill(target: common::PgTarget) -> Result<()> {
    let db = common::TestDb::start(target.clone(), "crash_flush_exec_kill").await?;
    let reopen_target = db.target.clone();
    let table = db.create_indexed_items_table("exec_kill_items", 24).await?;
    let relation = table.relation.clone();
    let reference = db.relation("exec_kill_ref");
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
              hot_row_limit => 4,
              min_flush_rows => 1,
              max_rows_per_file => 8,
              migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[&relation, &db.storage_name],
        )
        .await
        .context("manage_table")?;

    common::fence_async_mirror(&db.client).await?;

    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.failpoint = 'wait:after_temp_object';"
        ))
        .await
        .context("arm database failpoint for flush executor")?;

    let coordinator = connect_peer(&db).await?;
    barrier_lock(&coordinator).await?;

    let job_id: String = db
        .client
        .query_one(
            "SELECT (koldstore.flush_table($1::text::regclass, true)->>'job_id')",
            &[&relation],
        )
        .await
        .context("enqueue flush_table")?
        .get(0);

    let pids = common::wait_for_flush_executor_pids(&db.client, Duration::from_secs(30))
        .await
        .context("wait for flush executor")?;
    wait_until_barrier_waiter(&coordinator, || false)
        .await
        .context("wait for executor failpoint barrier")?;

    // SIGKILL of a connected bgworker triggers postmaster crash recovery.
    for pid in &pids {
        common::sigkill_pid(*pid).with_context(|| format!("SIGKILL flush executor pid={pid}"))?;
    }

    // Keep `db` alive for storage_root; reconnect after crash recovery.
    let client = common::wait_for_postgres(&reopen_target)
        .await
        .context("wait for postgres after flush-executor SIGKILL")?;
    // Crash restart drops session state; clear DB-level wait so a retry cannot re-park.
    // Finish recovery inline in this backend (no second bgworker).
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
        .context("recover_segments after executor kill")?;

    let retry_job: String = client
        .query_one(
            "SELECT (koldstore.flush_table($1::text::regclass, true)->>'job_id')",
            &[&relation],
        )
        .await
        .context("retry flush_table after kill")?
        .get(0);

    wait_for_job_terminal(&client, &retry_job, Duration::from_secs(60)).await?;
    wait_for_job_not_stuck_running(&client, &job_id).await?;

    let compare_cols = ["id", "account_id", "title", "qty", "category"];
    assert_recovered_flush_data_plane(
        &client,
        &relation,
        &db.storage_root,
        RecoveredFlushExpect {
            visible_rows: 24,
            expect_hot_fully_pruned: true,
            min_cold_segments: 1,
            reference: Some((reference.as_str(), &compare_cols)),
        },
    )
    .await
    .context("post executor kill data-plane checks")?;
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
            "error" => bail!("job {job_id} ended in error after executor kill recovery"),
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
            bail!("job {job_id} stuck in running after executor SIGKILL");
        }
    }
    Ok(())
}
