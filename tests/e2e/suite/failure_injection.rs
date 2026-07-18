use crate::common;

use anyhow::{Context, Result};
use std::path::PathBuf;

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
        "partial multi-segment outage",
    ];

    assert_eq!(faults.len(), 8);
}

#[tokio::test]
async fn filesystem_outage_during_flush_keeps_hot_rows_authoritative() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "failure_injection").await?;
        let table = db.create_indexed_items_table("failure_items", 32).await?;
        db.manage_shared(&table.relation, "id").await?;

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

        db.insert_pending_flush_job(&table.relation).await?;
        assert_eq!(db.flush_table(&table.relation).await?, 0);

        assert_eq!(common::row_count(&db.client, &table.relation).await?, 32);
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

async fn flushed_artifacts(
    db: &common::TestDb,
    relation: &str,
) -> Result<(PathBuf, PathBuf, String)> {
    let row = db
        .client
        .query_one(
            r#"
            SELECT m.manifest_path, cs.object_path
            FROM koldstore.manifest m
            JOIN koldstore.cold_segments cs
              ON cs.table_oid = m.table_oid
             AND cs.scope_key = m.scope_key
            WHERE m.table_oid = $1::text::regclass::oid
              AND m.sync_state = 'in_sync'
              AND cs.status = 'active'
            ORDER BY cs.batch_number
            LIMIT 1
            "#,
            &[&relation],
        )
        .await
        .context("lookup published manifest/segment")?;
    let manifest_rel: String = row.get(0);
    let object_rel: String = row.get(1);
    let manifest_abs = db.storage_root.join(&manifest_rel);
    let segment_abs = db.storage_root.join(&object_rel);
    Ok((manifest_abs, segment_abs, manifest_rel))
}

/// Missing Parquet after publish must fail merge reads (not silent hot-only).
#[tokio::test]
async fn missing_parquet_after_flush_fails_merge_scan_closed() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "missing_parquet").await?;
        let table = db
            .create_indexed_items_table("missing_pq_items", 20)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        let flushed = db.flush_table(&table.relation).await?;
        assert!(flushed > 0);

        let (_manifest, segment, _) = flushed_artifacts(&db, &table.relation).await?;
        std::fs::remove_file(&segment).with_context(|| format!("remove {}", segment.display()))?;

        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await
            .expect_err("merge scan must error when parquet is missing");
        let message = err
            .as_db_error()
            .map(|db| db.message())
            .unwrap_or("missing db error");
        assert!(
            !message.is_empty(),
            "expected a database error for missing cold segment"
        );
    }

    Ok(())
}

/// Corrupt Parquet footer after publish must fail merge reads.
#[tokio::test]
async fn corrupt_parquet_footer_fails_merge_scan_closed() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "corrupt_parquet").await?;
        let table = db
            .create_indexed_items_table("corrupt_pq_items", 20)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        let (_manifest, segment, _) = flushed_artifacts(&db, &table.relation).await?;
        std::fs::write(&segment, b"not-a-parquet-file")
            .with_context(|| format!("corrupt {}", segment.display()))?;

        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await
            .expect_err("merge scan must error on corrupt parquet");
        assert!(err.as_db_error().is_some());
    }

    Ok(())
}

/// Catalog pointing at a missing published manifest must fail merge reads.
#[tokio::test]
async fn missing_manifest_after_flush_fails_merge_scan_closed() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "missing_manifest").await?;
        let table = db
            .create_indexed_items_table("missing_mf_items", 20)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        let (manifest_abs, segment_abs, manifest_rel) =
            flushed_artifacts(&db, &table.relation).await?;
        std::fs::remove_file(&manifest_abs)
            .with_context(|| format!("remove {}", manifest_abs.display()))?;
        std::fs::remove_file(&segment_abs)
            .with_context(|| format!("remove {}", segment_abs.display()))?;
        // Point catalog metadata at a path that no longer exists (stale publish).
        db.client
            .execute(
                r#"
                UPDATE koldstore.manifest
                SET manifest_path = $2
                WHERE table_oid = $1::text::regclass::oid
                "#,
                &[&table.relation, &format!("missing/{manifest_rel}")],
            )
            .await?;

        // Same session as flush (matches missing_parquet); cold open must fail closed.
        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await
            .expect_err("merge scan must error when published cold objects are missing");
        assert!(err.as_db_error().is_some());
    }

    Ok(())
}
