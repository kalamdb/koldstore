use crate::common;

use anyhow::{Context, Result};

#[tokio::test]
async fn flush_object_outage_does_not_publish_partial_cold_state_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_object_outage").await?;
        let table = db
            .create_indexed_items_table("object_outage_items", 20)
            .await?;
        db.manage_shared(&table.relation, "id").await?;

        let blocking_file = db.storage_root.join("blocked");
        std::fs::write(&blocking_file, b"not a directory")
            .with_context(|| format!("write {}", blocking_file.display()))?;
        let blocking_path = blocking_file
            .to_str()
            .context("blocking path must be valid utf-8")?;
        db.client
            .query_one(
                "SELECT koldstore.alter_storage_location($1, $2, '{}'::jsonb)",
                &[&db.storage_name, &blocking_path],
            )
            .await?;
        db.insert_pending_flush_job(&table.relation).await?;

        // Storage root is a plain file: Nested flush records status=error.
        // wait_for_flush_job_terminal surfaces that as Err (not Ok(0)).
        let flush_err = db
            .flush_table(&table.relation)
            .await
            .expect_err("object outage must fail the flush job");
        assert!(
            flush_err.to_string().contains("status=error")
                || flush_err.to_string().contains("File exists")
                || flush_err.to_string().contains("create storage root"),
            "unexpected outage error: {flush_err:#}"
        );
        assert_eq!(common::row_count(&db.client, &table.relation).await?, 20);
        assert_eq!(
            common::cold_segment_count(&db.client, &table.relation).await?,
            0
        );
        assert_eq!(
            common::published_manifest_count(&db.client, &table.relation).await?,
            0
        );
        assert_eq!(
            common::active_job_count(&db.client, &table.relation).await?,
            0
        );
        let job = db
            .client
            .query_one(
                "SELECT status, phase, error_trace FROM koldstore.jobs WHERE table_oid = $1::text::regclass::oid AND job_type = 'flush' ORDER BY updated_at DESC LIMIT 1",
                &[&table.relation],
            )
            .await?;
        assert_eq!(job.get::<_, String>(0), "error");
        assert_eq!(job.get::<_, String>(1), "failed");
        assert!(job.get::<_, Option<String>>(2).is_some());
    }

    Ok(())
}
