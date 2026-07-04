use koldstore_manifest::{
    FilesState, Manifest, ManifestBloomFilter, ManifestColumnStats, ManifestSegment, PkFilter,
    SegmentStatus, SyncState,
};
use serde_json::json;

#[test]
fn manifest_serializes_kalamdb_compatible_shape() {
    let mut manifest = Manifest::new_shared("app", "items", 2);
    manifest.append_segment(ManifestSegment::committed(
        0,
        "batch-0.parquet",
        1..=10,
        100..=110,
        10,
        4096,
        2,
    ));

    let json = manifest.to_json_value().unwrap();

    assert_eq!(json["version"], 1);
    assert_eq!(json["namespace"], "app");
    assert_eq!(json["table"], "items");
    assert_eq!(json["scope_id"], serde_json::Value::Null);
    assert_eq!(json["max_seq"], 10);
    assert_eq!(json["max_commit_seq"], 110);
    assert_eq!(json["segments"][0]["status"], "committed");
}

#[test]
fn manifest_round_trip_preserves_files_state_and_pk_filter() {
    let mut manifest = Manifest::new_user("app", "notes", "user-a", 1);
    manifest.files = FilesState {
        current_subfolder: "files-0".to_string(),
        subfolder_count: 1,
        max_files_per_subfolder: 1000,
        total_files: Some(7),
    };
    let mut segment =
        ManifestSegment::committed(1, "batch-1.parquet", 20..=30, 120..=130, 11, 8192, 1);
    segment.pk_filter = Some(PkFilter::exact(vec![0, 1]));
    manifest.append_segment(segment);

    let encoded = serde_json::to_string(&manifest).unwrap();
    let decoded: Manifest = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.scope_id.as_deref(), Some("user-a"));
    assert_eq!(decoded.files.total_files, Some(8));
    assert_eq!(
        decoded.segments[0].pk_filter.as_ref().unwrap().kind,
        "exact"
    );
}

#[test]
fn manifest_round_trip_preserves_indexed_column_stats_and_bloom_filters() {
    let mut manifest = Manifest::new_shared("app", "items", 1);
    let mut segment =
        ManifestSegment::committed(1, "batch-1.parquet", 20..=30, 120..=130, 11, 8192, 1);
    segment.column_stats.insert(
        "created_at".to_string(),
        ManifestColumnStats::new(json!("2026-01-01T00:00:00Z"), json!("2026-01-31T00:00:00Z")),
    );
    segment.bloom_filters.push(ManifestBloomFilter::bloom(
        vec!["id".to_string()],
        Some(0.01),
    ));
    manifest.append_segment(segment);

    let encoded = serde_json::to_string(&manifest).unwrap();
    let decoded: Manifest = serde_json::from_str(&encoded).unwrap();

    assert_eq!(
        decoded.segments[0].column_stats["created_at"].min,
        json!("2026-01-01T00:00:00Z")
    );
    assert_eq!(decoded.segments[0].bloom_filters[0].kind, "bloom");
    assert_eq!(decoded.segments[0].bloom_filters[0].columns, vec!["id"]);
}

#[test]
fn manifest_batch_append_reserves_once_and_updates_watermarks_once_per_flush() {
    let mut manifest = Manifest::new_shared("app", "items", 1);
    let segments = vec![
        ManifestSegment::committed(1, "batch-1.parquet", 1..=10, 11..=20, 10, 1024, 1),
        ManifestSegment::committed(2, "batch-2.parquet", 11..=30, 21..=40, 20, 2048, 1),
    ];

    let update = manifest.append_segment_batch(segments);

    assert_eq!(update.appended_segments, 2);
    assert_eq!(update.manifest_writes_required, 1);
    assert_eq!(manifest.segments.len(), 2);
    assert_eq!(manifest.max_seq, 30);
    assert_eq!(manifest.max_commit_seq, 40);
    assert_eq!(manifest.files.total_files, Some(2));
}

#[test]
fn sync_state_transitions_match_flush_contract() {
    assert!(SyncState::PendingWrite.can_transition_to(SyncState::Syncing));
    assert!(SyncState::Syncing.can_transition_to(SyncState::InSync));
    assert!(SyncState::Syncing.can_transition_to(SyncState::Error));
    assert!(SyncState::Error.can_transition_to(SyncState::PendingWrite));
    assert!(!SyncState::InSync.can_transition_to(SyncState::Syncing));
}

#[test]
fn deleted_manifest_segment_does_not_contribute_to_max_watermarks() {
    let mut manifest = Manifest::new_shared("app", "items", 1);
    let mut deleted =
        ManifestSegment::committed(0, "batch-0.parquet", 1..=100, 1..=100, 100, 1024, 1);
    deleted.status = SegmentStatus::Deleted;
    manifest.append_segment(deleted);

    assert_eq!(manifest.max_seq, 0);
    assert_eq!(manifest.max_commit_seq, 0);
}

#[test]
fn golden_manifest_fixture_remains_compatible() {
    let golden = include_str!("../../../tests/golden/manifest-v1.json");
    let parsed: Manifest = serde_json::from_str(golden).unwrap();
    let value = serde_json::to_value(&parsed).unwrap();

    assert_eq!(parsed.version, 1);
    assert_eq!(parsed.namespace.as_deref(), Some("app"));
    assert_eq!(parsed.table, "items");
    assert_eq!(parsed.segments.len(), 1);
    assert_eq!(
        value["segments"][0]["pk_filter"],
        json!({
            "kind": "exact",
            "column_ids": [0],
            "false_positive_rate": null
        })
    );
}
