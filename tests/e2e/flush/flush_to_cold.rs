#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};

#[test]
fn flush_to_cold_plan_writes_parquet_manifest_segments_and_pk_hints() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    use koldstore_common::{CommitSeq, ScopeKey, SeqId, StablePkHash};
    use koldstore_flush::job::{
        plan_cold_pk_hint_updates, plan_cold_segment_insert, FlushBatchInput, HotRowCandidate,
    };
    use koldstore_parquet::{ColumnStats, SegmentFooterMetadata};
    use serde_json::json;

    let batch = FlushBatchInput {
        batch_size: 100,
        rows: vec![
            HotRowCandidate::live(
                StablePkHash::from_hex("01").unwrap(),
                SeqId::new(1).unwrap(),
                CommitSeq::new(11).unwrap(),
            ),
            HotRowCandidate::live(
                StablePkHash::from_hex("02").unwrap(),
                SeqId::new(2).unwrap(),
                CommitSeq::new(12).unwrap(),
            ),
            HotRowCandidate::tombstone(
                StablePkHash::from_hex("03").unwrap(),
                SeqId::new(3).unwrap(),
                CommitSeq::new(13).unwrap(),
            ),
        ],
    }
    .plan();
    let footer = SegmentFooterMetadata::from_footer(
        &batch.footer_summary(),
        batch.live_rows as u64,
        4096,
        1,
        vec![(
            "_seq".to_string(),
            ColumnStats {
                min: json!(1),
                max: json!(2),
            },
        )],
    )
    .unwrap();
    let segment = plan_cold_segment_insert(
        42,
        Some(ScopeKey::new("tenant-a").unwrap()),
        "app/items/batch-0.parquet",
        footer,
        "manifest-etag-1",
    )
    .unwrap();
    let hints = plan_cold_pk_hint_updates(
        42,
        Some(ScopeKey::new("tenant-a").unwrap()),
        &batch,
        "exact",
    );

    assert_eq!(batch.live_rows, 2);
    assert_eq!(batch.tombstones_retained, 1);
    assert_eq!(segment.object_path, "app/items/batch-0.parquet");
    assert_eq!(segment.status, "active");
    assert_eq!(segment.manifest_etag, "manifest-etag-1");
    assert_eq!(segment.scope_key.as_ref().unwrap().as_str(), "tenant-a");
    assert_eq!(segment.min_seq.get(), 1);
    assert_eq!(segment.max_commit_seq.get(), 12);
    assert_eq!(hints.len(), 2);
    assert!(hints.iter().all(|hint| hint.hint_kind == "exact"));
    assert!(hints
        .iter()
        .all(|hint| hint.scope_key.as_ref().unwrap().as_str() == "tenant-a"));
}

#[tokio::test]
async fn flush_to_cold_writes_catalog_manifest_and_parquet_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_to_cold").await?;
        let table = db.create_indexed_items_table("flush_items", 64).await?;
        db.migrate_shared(&table.relation, "id").await?;

        let flushed = db.flush_table(&table.relation).await?;
        assert_eq!(flushed, 64);
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;
        common::assert_no_active_jobs(&db.client, &table.relation).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 64).await?;

        let artifact = db
            .client
            .query_one(
                r#"
                SELECT m.manifest_path, cs.object_path, cs.row_count, cs.byte_size
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

        let file = std::fs::File::open(&parquet_path)?;
        let reader = SerializedFileReader::new(file)?;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 64);
    }

    Ok(())
}
