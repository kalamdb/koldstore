use crate::common;

use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};

#[test]
fn flush_to_cold_plan_writes_pending_segment_batch_sql() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let statement = koldstore_flush::plan_flush_segments_batch_insert().unwrap();
    let sql = statement.sql.to_ascii_lowercase();
    assert!(sql.contains("unnest("));
    assert!(sql.contains("koldstore.cold_segments"));
    assert!(
        sql.contains("pending"),
        "production insert must stage segments as pending before activate CAS"
    );
}

#[tokio::test]
async fn flush_to_cold_writes_catalog_manifest_and_parquet_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_to_cold").await?;
        let table = db.create_indexed_items_table("flush_items", 64).await?;
        db.manage_shared(&table.relation, "id").await?;

        let flushed = db.flush_table(&table.relation).await?;
        assert_eq!(flushed, 64);
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;
        common::assert_no_active_jobs(&db.client, &table.relation).await?;

        let publish_meta = db
            .client
            .query_one(
                r#"
                SELECT m.generation,
                       count(*) FILTER (WHERE cs.status = 'active')::bigint,
                       count(*) FILTER (WHERE cs.status = 'pending')::bigint,
                       bool_and(cs.checksum IS NOT NULL AND length(cs.checksum) = 64)
                FROM koldstore.manifest m
                JOIN koldstore.cold_segments cs
                  ON cs.table_oid = m.table_oid
                 AND cs.scope_key = m.scope_key
                WHERE m.table_oid = $1::text::regclass::oid
                GROUP BY m.generation
                "#,
                &[&table.relation],
            )
            .await?;
        let generation: i64 = publish_meta.get(0);
        let active: i64 = publish_meta.get(1);
        let pending: i64 = publish_meta.get(2);
        let checksums_ok: bool = publish_meta.get(3);
        assert!(
            generation >= 1,
            "generation must be CAS-bumped, got {generation}"
        );
        assert!(active > 0);
        assert_eq!(pending, 0, "no pending segments after successful flush");
        assert!(checksums_ok, "active segments must store sha256 checksum");
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 64).await?;

        let artifact = db
            .client
            .query_one(
                &format!(
                    r#"
                SELECT
                  {manifest},
                  {object},
                  cs.row_count,
                  cs.byte_size,
                  cs.path
                FROM koldstore.manifest m
                JOIN pg_class c ON c.oid = m.table_oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN koldstore.cold_segments cs
                  ON cs.table_oid = m.table_oid
                 AND cs.scope_key = m.scope_key
                WHERE m.table_oid = $1::text::regclass::oid
                  AND m.generation > 0
                  AND m.sync_state = 'in_sync'
                  AND cs.status = 'active'
                ORDER BY cs.batch_number
                LIMIT 1
                "#,
                    manifest = common::SQL_DEFAULT_MANIFEST_OBJECT_KEY,
                    object = common::SQL_DEFAULT_COLD_OBJECT_KEY,
                ),
                &[&table.relation],
            )
            .await?;
        let manifest_path = db.storage_root.join(artifact.get::<_, String>(0));
        let parquet_path = db.storage_root.join(artifact.get::<_, String>(1));
        assert!(
            manifest_path.exists(),
            "missing {}",
            manifest_path.display()
        );
        assert!(parquet_path.exists(), "missing {}", parquet_path.display());
        assert_eq!(artifact.get::<_, i64>(2), 64);
        assert!(artifact.get::<_, i64>(3) > 0);
        let relative_path = artifact.get::<_, String>(4);
        assert!(
            relative_path.starts_with("001/") && relative_path.ends_with(".parquet"),
            "cold_segments.path must be table-relative, got {relative_path}"
        );

        let file = std::fs::File::open(&parquet_path)?;
        let reader = SerializedFileReader::new(file)?;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 64);
    }

    Ok(())
}
