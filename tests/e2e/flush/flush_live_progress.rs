//! Job listing and live progress tracking while a flush is in flight.
//!
//! Queue mode commits catalog SPI boundaries between phases, so a peer session
//! can observe `koldstore.jobs` mid-flush. Inline/`Nested` keeps claim→finish in
//! one transaction and is not used here for live visibility.

use crate::common;
use crate::flush::harness::{
    barrier_lock, barrier_unlock, connect_peer, wait_until_barrier_waiter_deadline,
};
use anyhow::{Context, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn list_jobs_and_flush_progress_fields_are_populated() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_list_jobs").await?;
        let table = db.create_indexed_items_table("list_jobs_items", 48).await?;
        db.manage_shared(&table.relation, "id").await?;
        db.client
            .batch_execute(&format!(
                "SELECT koldstore.set_table_auto_flush('{relation}'::regclass, false)",
                relation = table.relation
            ))
            .await
            .context("set_table_auto_flush(false)")?;

        let job_id = common::flush_table_job_id(&db.client, &table.relation, false)
            .await
            .context("flush_table")?
            .context("expected flush job id")?;

        let row = db
            .client
            .query_one(
                r#"
                SELECT status, phase, progress_current, progress_total, rows_flushed
                FROM koldstore.jobs
                WHERE id = $1::text::uuid
                "#,
                &[&job_id],
            )
            .await
            .context("read job")?;
        assert_eq!(row.get::<_, String>("status"), "completed");
        assert_eq!(row.get::<_, String>("phase"), "finished");
        assert!(row.get::<_, i64>("progress_current") > 0);
        assert_eq!(
            row.get::<_, i64>("progress_total"),
            row.get::<_, i64>("rows_flushed"),
            "progress_total should match selected/flushed rows, not full mirror backlog"
        );
        assert!(row.get::<_, i64>("rows_flushed") > 0);

        let listed = db
            .client
            .query_one(
                r#"
                SELECT koldstore.list_jobs(
                  statuses => '["completed"]'::jsonb,
                  job_types => '["flush"]'::jsonb,
                  table_name => $1::text::regclass
                )::text
                "#,
                &[&table.relation],
            )
            .await
            .context("list_jobs")?
            .get::<_, String>(0);
        let jobs: serde_json::Value = serde_json::from_str(&listed)?;
        assert!(
            jobs.as_array().is_some_and(|arr| {
                arr.iter().any(|job| {
                    job.get("id").and_then(|v| v.as_str()) == Some(job_id.as_str())
                        && job
                            .get("progress_total")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0)
                            > 0
                        && job.get("progress_unit").is_none()
                })
            }),
            "list_jobs should include completed flush with progress, got {listed}"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_flush_exposes_live_job_progress_while_parked() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_live_mid").await?;
        let table = db.create_indexed_items_table("live_prog_items", 64).await?;
        let dbname: String = db
            .client
            .query_one("SELECT current_database()::text", &[])
            .await?
            .get(0);

        // Override fixture `inline` for queue executors (they inherit database GUCs).
        // Do not arm the failpoint yet — manage/fence spawn the async mirror worker,
        // which must not inherit a flush wait barrier.
        // min_max_rows_per_file must be database-level: the one-shot executor does
        // not inherit session SET, and manage_table's max_rows_per_file=16 would
        // otherwise fail validation inside the executor before after_select_rows.
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
                  hot_row_limit => 8,
                  min_flush_rows => 1,
                  max_rows_per_file => 16,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await
            .context("manage_table")?;
        common::fence_async_mirror(&db.client).await?;

        // Arm after manage/fence so only the flush executor inherits the wait.
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.failpoint = 'wait:after_select_rows';"
            ))
            .await
            .context("arm database failpoint for flush executor")?;

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let job_id: String = db
            .client
            .query_one(
                "SELECT (koldstore.flush_table($1::text::regclass, true)->>'job_id')",
                &[&table.relation],
            )
            .await
            .context("enqueue flush_table")?
            .get(0);
        anyhow::ensure!(!job_id.is_empty() && job_id != "null", "expected job uuid");

        common::wait_for_flush_executor_pids(&db.client, Duration::from_secs(30))
            .await
            .context("wait for flush executor")?;

        // Stop waiting early if the job leaves running without parking (missed
        // failpoint / early error) so the failure is actionable.
        let job_left_running = Arc::new(AtomicBool::new(false));
        let probe = connect_peer(&db).await?;
        let probe_job = job_id.clone();
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
            Duration::from_secs(45),
        )
        .await;
        probe_handle.abort();
        if let Err(error) = wait_result {
            let status: String = db
                .client
                .query_one(
                    "SELECT status || ':' || coalesce(phase, '') || ':' || coalesce(error_trace, '') \
                     FROM koldstore.jobs WHERE id = $1::text::uuid",
                    &[&job_id],
                )
                .await
                .map(|row| row.get(0))
                .unwrap_or_else(|_| "<unreadable>".to_string());
            barrier_unlock(&coordinator).await.ok();
            let _ = db
                .client
                .batch_execute(&format!(
                    "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint; \
                     RESET koldstore.failpoint;"
                ))
                .await;
            return Err(error).context(format!(
                "executor did not park at after_select_rows (job={job_id} state={status})"
            ));
        }

        // While the executor is parked, peer must see committed running progress.
        let live = db
            .client
            .query_one(
                r#"
                SELECT status, phase, progress_total, progress_current,
                       attempt_token IS NOT NULL AS has_attempt
                FROM koldstore.jobs
                WHERE id = $1::text::uuid
                "#,
                &[&job_id],
            )
            .await
            .context("read live job while parked")?;
        assert_eq!(live.get::<_, String>("status"), "running");
        assert!(live.get::<_, bool>("has_attempt"));
        let progress_total: i64 = live.get("progress_total");
        anyhow::ensure!(
            progress_total > 0,
            "progress_total must be stamped at claim before encode finishes, got {progress_total}"
        );
        let phase: String = live.get("phase");
        anyhow::ensure!(
            !phase.is_empty() && phase != "finished",
            "mid-flush phase should not be finished, got {phase}"
        );

        // Release the executor and let it finish.
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint; \
                 RESET koldstore.failpoint;"
            ))
            .await
            .context("clear database failpoint")?;
        barrier_unlock(&coordinator).await?;

        wait_for_job_terminal(&db.client, &job_id, Duration::from_secs(60)).await?;

        let final_row = db
            .client
            .query_one(
                r#"
                SELECT status, phase, progress_current, progress_total, rows_flushed, error_trace
                FROM koldstore.jobs
                WHERE id = $1::text::uuid
                "#,
                &[&job_id],
            )
            .await?;
        assert_eq!(final_row.get::<_, String>("status"), "completed");
        assert_eq!(final_row.get::<_, String>("phase"), "finished");
        assert!(final_row.get::<_, Option<String>>("error_trace").is_none());
        assert!(final_row.get::<_, i64>("rows_flushed") > 0);
        assert!(final_row.get::<_, i64>("progress_current") > 0);

        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_execution; \
                 ALTER DATABASE \"{dbname}\" RESET koldstore.min_max_rows_per_file; \
                 RESET koldstore.flush_execution; \
                 RESET koldstore.min_max_rows_per_file;"
            ))
            .await
            .ok();
    }

    Ok(())
}

async fn wait_for_job_terminal(
    client: &tokio_postgres::Client,
    job_id: &str,
    deadline: Duration,
) -> Result<()> {
    let started = Instant::now();
    loop {
        let status: String = client
            .query_one(
                "SELECT status FROM koldstore.jobs WHERE id = $1::text::uuid",
                &[&job_id],
            )
            .await?
            .get(0);
        if matches!(status.as_str(), "completed" | "error" | "cancelled") {
            anyhow::ensure!(
                status == "completed",
                "flush job ended as {status}, expected completed"
            );
            return Ok(());
        }
        if started.elapsed() > deadline {
            anyhow::bail!("job {job_id} still {status} after {deadline:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
