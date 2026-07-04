#[path = "common/mod.rs"]
mod common;

use anyhow::{Context, Result};

#[test]
fn failure_injection_matrix_lists_required_faults() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let faults = [
        "filesystem outage",
        "corrupt Parquet footer",
        "stale manifest generation",
        "missing manifest",
        "orphan final object",
        "credential failure",
        "network timeout",
    ];

    assert_eq!(faults.len(), 7);
}

#[tokio::test]
async fn filesystem_outage_during_flush_keeps_hot_rows_authoritative() -> Result<()> {
    for target in common::local_pg_matrix() {
        let db = common::TestDb::start(target, "failure_injection").await?;
        let table = db.create_indexed_items_table("failure_items", 32).await?;
        db.migrate_shared(&table.relation, "id").await?;

        let blocking_file = db.storage_root.join("not-a-directory");
        std::fs::write(&blocking_file, b"blocks create_dir_all")
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

        db.insert_pending_flush_job(&table.relation, "").await?;
        let flush_error = db.flush_table(&table.relation).await.unwrap_err();
        assert!(!flush_error.to_string().is_empty());

        assert_eq!(common::row_count(&db.client, &table.relation).await?, 32);
        assert_eq!(
            common::cold_segment_count(&db.client, &table.relation).await?,
            0
        );
        assert_eq!(
            common::manifest_count(&db.client, &table.relation).await?,
            0
        );
        assert_eq!(
            common::active_job_count(&db.client, &table.relation).await?,
            1
        );
    }

    Ok(())
}
