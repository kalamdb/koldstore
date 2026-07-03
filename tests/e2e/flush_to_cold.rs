#[test]
fn flush_to_cold_plan_writes_parquet_manifest_segments_and_pk_hints() {
    use koldstore_core::{CommitSeq, ScopeKey, SeqId, StablePkHash};
    use koldstore_parquet::{ColumnStats, SegmentFooterMetadata};
    use pg_koldstore::flush::job::{
        plan_cold_pk_hint_updates, plan_cold_segment_insert, FlushBatchInput, HotRowCandidate,
    };
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
