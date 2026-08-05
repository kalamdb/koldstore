//! SIGKILL a queue-mode flush executor mid-flush, then reclaim and finish.
//!
//! Gated by `KOLDSTORE_CRASH_FLUSH_EXECUTOR=1` (destructive; not CI-default).
//! Arms a database-level `wait:` failpoint so the one-shot executor inherits it,
//! parks at the barrier, kills only the executor PID from this test process,
//! recovers, and verifies the table remains readable with the job not stuck.

use crate::common;
use crate::flush::harness::{
    barrier_lock, barrier_unlock, connect_peer, wait_until_barrier_waiter,
};

use anyhow::{bail, Context, Result};
use std::time::Duration;
use tokio::time::sleep;

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

    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        run_executor_kill(target).await?;
    }
    Ok(())
}

async fn run_executor_kill(target: common::PgTarget) -> Result<()> {
    let db = common::TestDb::start(target, "crash_flush_exec_kill").await?;
    let table = db.create_indexed_items_table("exec_kill_items", 24).await?;
    let reference = db.relation("exec_kill_ref");
    common::create_reference_clone(&db.client, &table.relation, &reference, &["id"]).await?;

    let dbname: String = db
        .client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);

    // Queue mode + short txns: executor is a separate backend we can SIGKILL.
    // Override the E2E fixture default (`inline`) for both this session and new
    // backends (flush executors do not inherit session GUCs).
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'queue'; \
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
            &[&table.relation, &db.storage_name],
        )
        .await
        .context("manage_table")?;

    common::fence_async_mirror(&db.client).await?;

    // Arm wait on the database so the flush executor inherits it (session SET
    // on this client does not affect background workers).
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
            "SELECT koldstore.flush_table($1::text::regclass, true)::text",
            &[&table.relation],
        )
        .await
        .context("enqueue flush_table")?
        .get(0);

    // Wait until an executor is visible, then until it parks on the barrier.
    let pids = common::wait_for_flush_executor_pids(&db.client, Duration::from_secs(30))
        .await
        .context("wait for flush executor")?;
    wait_until_barrier_waiter(&coordinator, || false)
        .await
        .context("wait for executor failpoint barrier")?;

    // Kill only the executor backend(s) — not the postmaster / test client.
    for pid in &pids {
        common::sigkill_pid(*pid).with_context(|| format!("SIGKILL flush executor pid={pid}"))?;
    }
    common::wait_until_no_flush_executors(&db.client, Duration::from_secs(15)).await?;

    barrier_unlock(&coordinator).await.ok();

    // Disarm before reclaim / retry so a new executor does not re-park.
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint; \
             RESET koldstore.failpoint;"
        ))
        .await
        .context("clear database failpoint")?;

    let _ = db
        .client
        .query_one(
            "SELECT koldstore.recover_segments($1::text::regclass, false)",
            &[&table.relation],
        )
        .await
        .context("recover_segments after executor kill")?;

    // Re-enqueue / spawn: claim path reclaims orphan running jobs.
    let retry_job: String = db
        .client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass, true)::text",
            &[&table.relation],
        )
        .await
        .context("retry flush_table after kill")?
        .get(0);

    wait_for_job_terminal(&db.client, &retry_job, Duration::from_secs(60)).await?;
    wait_for_job_not_stuck_running(&db.client, &job_id).await?;

    common::fence_async_mirror(&db.client).await?;
    let visible = common::relation_row_count(&db.client, &table.relation).await?;
    if visible != 24 {
        bail!("after executor SIGKILL expected 24 visible rows, got {visible}");
    }
    common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;
    common::assert_no_active_jobs(&db.client, &table.relation).await?;
    // Reference oracle: managed content must still match the untouched heap twin.
    // Compare non-default columns — `created_at` may differ in type defaults after clone.
    common::assert_managed_matches_reference_ordered(
        &db.client,
        &table.relation,
        &reference,
        &["id", "account_id", "title", "qty", "category"],
    )
    .await?;

    let integrity_text: String = db
        .client
        .query_one(
            "SELECT koldstore.verify_table_integrity($1::text::regclass)::text",
            &[&table.relation],
        )
        .await
        .context("verify_table_integrity after executor kill")?
        .get(0);
    let integrity: serde_json::Value =
        serde_json::from_str(&integrity_text).context("parse verify_table_integrity json")?;
    let ok = integrity
        .get("ok")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !ok {
        bail!("verify_table_integrity reported failure: {integrity}");
    }
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
    // Original attempt may be reclaimed to pending then completed under a new
    // attempt, or marked error — anything but permanent ownerless running.
    let status: String = client
        .query_one(
            "SELECT status FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        )
        .await?
        .get(0);
    if status == "running" {
        // Give reclaim a moment via the retry flush path, then re-check.
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
