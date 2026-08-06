//! Flush policy E2E: min_flush_rows, max_rows_per_file gate, and enqueue skip.
//!
//! Complements pure unit tests in `koldstore-flush` with real manage → insert →
//! flush_table / jobs-table behavior.

use crate::common;
use anyhow::{Context, Result};

#[tokio::test]
async fn undersized_excess_below_max_rows_per_file_does_not_enqueue() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_policy_undersized").await?;
        let relation = db.relation("msgs");
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await
            .context("manage_table")?;

        // 1450 mirror rows → excess 450 < max_rows_per_file 1000 → no job.
        insert_rows(&db.client, &relation, 1, 1450).await?;
        common::fence_async_mirror(&db.client).await?;

        let job: Option<String> = db
            .client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass)::text",
                &[&relation],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            job.as_deref().is_none_or(|v| v.is_empty() || v == "null"),
            "undersized excess must not enqueue a flush job, got {job:?}"
        );
        anyhow::ensure!(
            completed_flush_jobs(&db.client, &relation).await? == 0,
            "no completed flush jobs expected for undersized excess"
        );

        // Grow to a full file of excess → flush must run and track progress.
        insert_rows(&db.client, &relation, 1451, 2000).await?;
        common::fence_async_mirror(&db.client).await?;
        let job_id = common::flush_table_job_id(&db.client, &relation, false)
            .await
            .context("flush after full-file excess")?
            .context("expected flush job after full-file excess")?;
        let row = db
            .client
            .query_one(
                r#"
                SELECT status, rows_flushed, progress_current, progress_total
                FROM koldstore.jobs
                WHERE id = $1::text::uuid
                "#,
                &[&job_id],
            )
            .await?;
        assert_eq!(row.get::<_, String>("status"), "completed");
        let flushed: i64 = row.get("rows_flushed");
        let progress_total: i64 = row.get("progress_total");
        anyhow::ensure!(
            flushed >= 1000,
            "expected at least one max_rows_per_file of rows, got rows_flushed={flushed}"
        );
        anyhow::ensure!(
            progress_total == flushed || progress_total >= 1000,
            "progress_total={progress_total} should track selected flush size, flushed={flushed}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn min_flush_rows_blocks_enqueue_until_threshold() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_policy_min_rows").await?;
        let relation = db.relation("msgs");
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 100,
                  min_flush_rows => 50,
                  max_rows_per_file => 10,
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await
            .context("manage_table")?;

        // excess 30 < min_flush_rows 50 → no enqueue even though > max_rows_per_file.
        insert_rows(&db.client, &relation, 1, 130).await?;
        common::fence_async_mirror(&db.client).await?;
        let job: Option<String> = db
            .client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass)::text",
                &[&relation],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            job.as_deref().is_none_or(|v| v.is_empty() || v == "null"),
            "excess below min_flush_rows must not enqueue, got {job:?}"
        );

        // excess 60 >= min_flush_rows 50 → flush.
        insert_rows(&db.client, &relation, 131, 160).await?;
        common::fence_async_mirror(&db.client).await?;
        let flushed = db.flush_table(&relation).await?;
        anyhow::ensure!(
            flushed >= 50,
            "expected flush once min_flush_rows met, got {flushed}"
        );
    }
    Ok(())
}

/// Pure policy contract (no Postgres): documents the file-size + min-flush gates.
#[test]
fn flush_policy_row_count_contract() {
    use koldstore_flush::policy::{policy_flush_row_count, FlushPolicy};

    let policy = FlushPolicy::RowLimit {
        hot_row_limit: 1_000,
        min_flush_rows: 1,
        max_rows_per_file: 1_000,
        max_rows_per_flush: 10_000,
    };
    assert_eq!(policy_flush_row_count(1_450, &policy), 0);
    assert_eq!(policy_flush_row_count(2_000, &policy), 1_000);

    let small_files = FlushPolicy::RowLimit {
        hot_row_limit: 1,
        min_flush_rows: 1,
        max_rows_per_file: 1,
        max_rows_per_flush: 10_000,
    };
    assert_eq!(policy_flush_row_count(3, &small_files), 2);
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
            "#,
            &[&relation],
        )
        .await?
        .get(0))
}
