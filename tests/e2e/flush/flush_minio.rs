//! MinIO/S3-backed flush + merge-scan E2E coverage.
//!
//! Opt-in via `KOLDSTORE_MINIO=1` (and a reachable MinIO with bucket
//! `koldstore-test`). Skipped when the gate is unset so filesystem-only local
//! runs stay green.

#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use koldstore_storage::StorageClient;
use parquet::file::reader::{FileReader, SerializedFileReader};

#[test]
fn flush_minio_gate_documents_opt_in_env() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    // Contract-only: documents the opt-in gate used by the async scenario.
    let _ = common::minio_enabled();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_to_minio_writes_objects_and_merge_scan_reads_them() -> Result<()> {
    if !common::minio_enabled() {
        eprintln!(
            "skipping MinIO E2E: set KOLDSTORE_MINIO=1 and start MinIO \
             (docker/run.sh --no-build or equivalent)"
        );
        return Ok(());
    }

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start_minio(target, "flush_minio").await?;
        let table = db.create_indexed_items_table("minio_items", 48).await?;
        db.manage_shared(&table.relation, "id").await?;

        let flushed = db.flush_table(&table.relation).await?;
        assert_eq!(flushed, 48);
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;
        common::assert_no_active_jobs(&db.client, &table.relation).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 48).await?;

        let (manifest_path, object_path) =
            db.assert_minio_cold_artifacts(&table.relation, 48).await?;
        assert!(
            manifest_path.ends_with("/manifest.json"),
            "unexpected manifest path {manifest_path}"
        );
        assert!(
            object_path.contains("segment-") && object_path.ends_with(".parquet"),
            "unexpected parquet path {object_path}"
        );

        let client = db.minio_client()?;
        let parquet_bytes = client.get(&object_path)?;
        let reader = SerializedFileReader::new(bytes::Bytes::from(parquet_bytes))?;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 48);

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, account_id, title, qty, category)
                VALUES (1001, 1, 'hot-after-minio-flush', 1, 'hot')
                ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDED.title;
                ANALYZE {relation};
                "#,
                relation = table.relation
            ))
            .await?;

        let plan = common::explain(
            &db.client,
            &format!(
                "SELECT id, title FROM {} WHERE title = 'item-000001'",
                table.relation
            ),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan)?;
        common::assert_kold_merge_scan_cold_reads(&plan, &manifest_path, 1)?;

        let cold = db
            .client
            .query_one(
                &format!(
                    "SELECT id, title FROM {} WHERE title = 'item-000001'",
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(cold.get::<_, i64>(0), 1);
        assert_eq!(cold.get::<_, String>(1), "item-000001");

        let hot = db
            .client
            .query_one(
                &format!(
                    "SELECT id, title FROM {} WHERE title = 'hot-after-minio-flush'",
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(hot.get::<_, i64>(0), 1001);
        assert_eq!(hot.get::<_, String>(1), "hot-after-minio-flush");

        let total = common::row_count(&db.client, &table.relation).await?;
        assert_eq!(total, 49);
    }

    Ok(())
}
