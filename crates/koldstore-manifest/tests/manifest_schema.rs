use koldstore_manifest::{
    FilesState, Manifest, ManifestBloomFilter, ManifestColumnIndex, ManifestSegment, ManifestShard,
    PkFilter, SegmentStatus, SyncState, MANIFEST_VERSION,
};
use serde_json::json;

#[test]
fn manifest_serializes_folder_sharded_working_shape() {
    let mut manifest = Manifest::new_shared("app", "items", 2);
    manifest.append_segment(ManifestSegment::committed(
        1,
        "001/segment-0001-aaaaaaaa.parquet",
        1..=10,
        10,
        4096,
        2,
    ));

    let json = manifest.to_json_value().unwrap();

    assert_eq!(json["version"], MANIFEST_VERSION);
    assert_eq!(json["namespace"], "app");
    assert_eq!(json["table"], "items");
    assert_eq!(json["scope_id"], serde_json::Value::Null);
    assert_eq!(json["max_seq"], 10);
    assert_eq!(json["segments"][0]["status"], "committed");
    assert_eq!(json["shards"], json!([]));
}

#[test]
fn manifest_round_trip_preserves_files_state_and_pk_filter() {
    let mut manifest = Manifest::new_user("app", "notes", "user-a", 1);
    manifest.files = FilesState {
        current_subfolder: "001".to_string(),
        subfolder_count: 1,
        max_files_per_subfolder: 100,
        total_files: Some(7),
    };
    let mut segment = ManifestSegment::committed(
        1,
        "001/segment-0001-aaaaaaaa.parquet",
        20..=30,
        11,
        8192,
        1,
    );
    segment.pk_filter = Some(PkFilter::exact(vec![1, 2]));
    manifest.append_segment(segment);

    let encoded = serde_json::to_string(&manifest).unwrap();
    let decoded: Manifest = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.scope_id.as_deref(), Some("user-a"));
    assert_eq!(decoded.files.total_files, Some(7));
    assert_eq!(
        decoded.segments[0].pk_filter.as_ref().unwrap().kind,
        "exact"
    );
}

#[test]
fn manifest_v2_round_trip_preserves_packed_row_group_indexes_and_bloom_filters() {
    let mut manifest = Manifest::new_shared("app", "items", 1);
    let mut segment = ManifestSegment::committed(
        1,
        "001/segment-0001-aaaaaaaa.parquet",
        20..=30,
        11,
        8192,
        1,
    );
    segment.row_group_count = 2;
    segment.row_group_row_counts = vec![5, 6];
    segment.row_group_min_seqs = vec![20, 26];
    segment.row_group_max_seqs = vec![25, 30];
    segment.column_indexes.push(ManifestColumnIndex {
        column_id: 1,
        type_oid: 20,
        codec_version: 1,
        min_value: Some("01aa".to_string()),
        max_value: Some("01ff".to_string()),
        row_group_min_values: vec![Some("01aa".to_string()), None],
        row_group_max_values: vec![Some("01bb".to_string()), None],
        row_group_null_counts: vec![Some(0), Some(6)],
    });
    segment
        .bloom_filters
        .push(ManifestBloomFilter::bloom(vec![1], Some(0.01)));
    manifest.append_segment(segment);

    let encoded = serde_json::to_string(&manifest).unwrap();
    let decoded: Manifest = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.segments[0].row_group_row_counts, vec![5, 6]);
    assert_eq!(
        decoded.segments[0].column_indexes[0].row_group_min_values,
        vec![Some("01aa".to_string()), None]
    );
    assert_eq!(decoded.segments[0].bloom_filters[0].kind, "bloom");
    assert_eq!(decoded.segments[0].bloom_filters[0].column_ids, vec![1]);
}

#[test]
fn manifest_batch_append_reserves_once_and_updates_watermarks_once_per_flush() {
    let mut manifest = Manifest::new_shared("app", "items", 1);
    let segments = vec![
        ManifestSegment::committed(
        1,
        "001/segment-0001-aaaaaaaa.parquet",
        1..=10,
        10,
        1024,
        1,
    ),
        ManifestSegment::committed(
        2,
        "001/segment-0002-bbbbbbbb.parquet",
        11..=30,
        20,
        2048,
        1,
    ),
    ];

    let update = manifest.append_segment_batch(segments);

    assert_eq!(update.appended_segments, 2);
    assert_eq!(update.manifest_writes_required, 1);
    assert_eq!(manifest.segments.len(), 2);
    assert_eq!(manifest.max_seq, 30);
    assert_eq!(manifest.files.total_files, Some(0));
}

#[test]
fn manifest_omits_unset_optional_fields_on_serialize() {
    let mut manifest = Manifest::new_shared("app", "items", 1);
    manifest.append_segment(ManifestSegment::committed(
        1,
        "001/segment-0001-aaaaaaaa.parquet",
        1..=10,
        10,
        4096,
        1,
    ));

    let json = manifest.to_json_value().unwrap();

    assert!(json.get("publish").is_none());
    assert!(json["segments"][0].get("temp_path").is_none());
    assert!(json["segments"][0].get("checksum").is_none());
    assert!(json["segments"][0].get("etag").is_none());
    assert_eq!(json["files"]["total_files"], 0);
}

#[test]
fn sync_state_transitions_match_flush_contract() {
    assert!(SyncState::PendingWrite.can_transition_to(SyncState::Syncing));
    assert!(SyncState::Syncing.can_transition_to(SyncState::InSync));
    assert!(SyncState::Syncing.can_transition_to(SyncState::Error));
    assert!(SyncState::Error.can_transition_to(SyncState::PendingWrite));
    assert!(!SyncState::InSync.can_transition_to(SyncState::Syncing));
    assert_eq!(SyncState::PendingWrite.as_str(), "pending_write");
    assert_eq!(SyncState::PendingWrite.start_flush(), SyncState::Syncing);
    assert_eq!(SyncState::Syncing.finish_success(false), SyncState::InSync);
    assert_eq!(
        SyncState::Syncing.finish_success(true),
        SyncState::PendingWrite
    );
    assert_eq!(SyncState::Syncing.finish_error(), SyncState::Error);
}

#[test]
fn deleted_manifest_segment_does_not_contribute_to_max_watermarks() {
    let mut manifest = Manifest::new_shared("app", "items", 1);
    let mut deleted = ManifestSegment::committed(
        1,
        "001/segment-0001-aaaaaaaa.parquet",
        1..=100,
        100,
        1024,
        1,
    );
    deleted.status = SegmentStatus::Deleted;
    manifest.append_segment(deleted);

    assert_eq!(manifest.max_seq, 0);
}

#[test]
fn golden_folder_sharded_fixtures_remain_compatible() {
    let root_golden = include_str!("../../../tests/golden/manifest-root.json");
    let shard_golden = include_str!("../../../tests/golden/manifest-shard.json");
    let root: Manifest = serde_json::from_str(root_golden).unwrap();
    let shard: ManifestShard = serde_json::from_str(shard_golden).unwrap();

    assert_eq!(root.version, MANIFEST_VERSION);
    assert!(root.segments.is_empty());
    assert_eq!(root.shards.len(), 1);
    assert!(root.shards[0].path.contains(&root.shards[0].content_sha256));
    assert_eq!(root.shards[0].content_sha256.len(), 64);
    assert_eq!(shard.segments.len(), 1);
    assert_eq!(shard.segments[0].row_group_row_counts, vec![5, 5]);
    assert_eq!(
        shard.segments[0].pk_filter.as_ref().unwrap().column_ids,
        vec![1]
    );
}
