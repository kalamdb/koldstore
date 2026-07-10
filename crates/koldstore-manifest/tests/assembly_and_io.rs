use koldstore_manifest::{
    build_manifest_segment_from_catalog_row, load_manifest_from_path, manifest_from_catalog_rows,
    manifest_paths, manifest_relative_segment_path, manifest_to_json_bytes, write_manifest_to_path,
    CatalogManifestSegmentRow, Manifest, SyncState,
};
use serde_json::json;

#[test]
fn catalog_rows_assemble_shared_manifest_with_pk_filter_and_relative_paths() {
    let rows = vec![CatalogManifestSegmentRow {
        object_path: "app/items/batch-1.parquet".to_string(),
        batch_number: 1,
        min_seq: 1,
        max_seq: 10,
        min_commit_seq: 1,
        max_commit_seq: 10,
        row_count: 10,
        byte_size: 128,
        schema_version: 2,
        column_stats: json!({"id": {"min": 1, "max": 10}}),
    }];

    let manifest = manifest_from_catalog_rows(
        "app",
        "items",
        2,
        &["id".to_string(), "tenant".to_string()],
        rows,
    )
    .unwrap();

    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(manifest.segments[0].path, "batch-1.parquet");
    assert_eq!(manifest.max_seq, 10);
    assert_eq!(
        manifest.segments[0]
            .pk_filter
            .as_ref()
            .map(|filter| filter.column_ids.clone()),
        Some(vec![1, 2])
    );
    assert_eq!(
        manifest_relative_segment_path("app", "items", "app/items/batch-2.parquet"),
        "batch-2.parquet"
    );
}

#[test]
fn manifest_paths_and_round_trip_io() {
    let dir = tempfile_dir();
    let (relative, absolute) = manifest_paths("app", "notes", dir.to_str().unwrap());
    assert_eq!(relative, "app/notes/manifest.json");

    let mut manifest = Manifest::new_shared("app", "notes", 1);
    let segment = build_manifest_segment_from_catalog_row(
        "app",
        "notes",
        &["id".to_string()],
        &CatalogManifestSegmentRow {
            object_path: "app/notes/batch-1.parquet".to_string(),
            batch_number: 1,
            min_seq: 5,
            max_seq: 5,
            min_commit_seq: 5,
            max_commit_seq: 5,
            row_count: 1,
            byte_size: 32,
            schema_version: 1,
            column_stats: json!({}),
        },
    )
    .unwrap();
    manifest.append_segment(segment);

    write_manifest_to_path(&absolute, &manifest).unwrap();
    let loaded = load_manifest_from_path(&absolute).expect("manifest should load");
    assert_eq!(loaded.segments.len(), 1);
    assert_eq!(loaded.max_seq, 5);
    assert!(!manifest_to_json_bytes(&loaded).unwrap().is_empty());
}

#[test]
fn pending_write_sync_state_matches_hot_dml_constant() {
    assert_eq!(SyncState::PendingWrite.as_str(), "pending_write");
    assert_eq!(SyncState::InSync.after_hot_dml(), SyncState::PendingWrite);
    assert_eq!(
        SyncState::PendingWrite.after_hot_dml(),
        SyncState::PendingWrite
    );
    assert_eq!(SyncState::Error.after_hot_dml(), SyncState::Error);
}

#[test]
fn catalog_reconciliation_preserves_segment_order_and_watermarks() {
    let rows = vec![
        CatalogManifestSegmentRow {
            object_path: "app/items/batch-1.parquet".to_string(),
            batch_number: 1,
            min_seq: 1,
            max_seq: 10,
            min_commit_seq: 1,
            max_commit_seq: 10,
            row_count: 10,
            byte_size: 128,
            schema_version: 1,
            column_stats: json!({}),
        },
        CatalogManifestSegmentRow {
            object_path: "app/items/batch-2.parquet".to_string(),
            batch_number: 2,
            min_seq: 11,
            max_seq: 20,
            min_commit_seq: 11,
            max_commit_seq: 20,
            row_count: 10,
            byte_size: 256,
            schema_version: 1,
            column_stats: json!({}),
        },
    ];
    let manifest =
        manifest_from_catalog_rows("app", "items", 1, &["id".to_string()], rows).unwrap();
    assert_eq!(manifest.segments.len(), 2);
    assert_eq!(manifest.segments[0].path, "batch-1.parquet");
    assert_eq!(manifest.segments[1].path, "batch-2.parquet");
    assert_eq!(manifest.max_seq, 20);
    assert_eq!(manifest.max_commit_seq, 20);
}

fn tempfile_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "koldstore-manifest-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
