//! Job listing and live progress tracking while a flush is in flight.
//!
//! Queue mode commits catalog SPI boundaries between phases, so a peer session
//! can observe `koldstore.jobs` mid-flush. Inline/`Nested` keeps claim→finish in
//! one transaction and is not used here for live visibility.

use crate::common;
use crate::flush::harness::{barrier_lock, barrier_unlock, connect_peer, wait_until_barrier_waiter};
use anyhow::{Context, Result};
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
            .context("flush_table")?;

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
                        && job.get("progress_total").and_then(|v| v.as_i64()).unwrap_or(0) > 0
                        && job.get("progress_unit").is_none()
                })
            }),
            "list_jobs should include completed flush with progress, got {listed}"
        );
    }

    Ok(())
}

#[tokio::test]
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

        // Queue + short SPI commits so peer SELECTs see mid-flush job rows.
        // Database-level failpoint so the one-shot executor inherits it.
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'queue'; \
                 ALTER DATABASE \"{dbname}\" SET koldstore.failpoint = 'wait:after_select_rows'; \
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
        anyhow::ensure!(!job_id.is_empty() && job_id != "null", "expected job uuid");

        common::wait_for_flush_executor_pids(&db.client, Duration::from_secs(30))
            .await
            .context("wait for flush executor")?;
        wait_until_barrier_waiter(&coordinator, || false)
            .await
            .context("wait for executor failpoint barrier")?;

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
                 RESET koldstore.flush_execution;"
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
        if matches!(
            status.as_str(),
            "completed" | "error" | "cancelled"
        ) {
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
