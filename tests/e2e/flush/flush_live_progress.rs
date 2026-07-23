//! Job listing and progress fields for flush jobs (Phase B).

use crate::common;
use anyhow::{Context, Result};

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
            .ok();

        let job_id = db
            .client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass)::text",
                &[&table.relation],
            )
            .await
            .context("flush_table")?
            .get::<_, String>(0);

        let row = db
            .client
            .query_one(
                r#"
                SELECT status, phase, progress_current, progress_total, progress_unit, rows_flushed
                FROM koldstore.jobs
                WHERE id = $1::text::uuid
                "#,
                &[&job_id],
            )
            .await
            .context("read job")?;
        assert_eq!(row.get::<_, String>("status"), "completed");
        assert_eq!(row.get::<_, String>("phase"), "finished");
        assert_eq!(row.get::<_, String>("progress_unit"), "rows");
        assert!(row.get::<_, i64>("progress_current") > 0);
        assert!(row.get::<_, i64>("progress_total") > 0);
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
                        && job.get("progress_unit").and_then(|v| v.as_str()) == Some("rows")
                })
            }),
            "list_jobs should include completed flush with progress, got {listed}"
        );
    }

    Ok(())
}
