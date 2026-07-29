//! Catalog-to-manifest assembly and local path I/O coverage.
//!
//! Folder-shard export rejection/round-trip edge cases live in `sharded_export.rs`.

use koldstore_catalog::CatalogSegmentIndexBound;
use koldstore_common::{ColumnId, ColumnRef};
use koldstore_manifest::{
    build_manifest_segment_from_catalog_row, manifest_from_catalog_rows, manifest_paths,
    manifest_relative_segment_path, manifest_to_json_bytes, try_load_manifest_from_path,
    write_manifest_to_path, CatalogManifestSegmentRow, Manifest, SyncState,
};
use koldstore_sortkey::{encode_sort_key_json, SortKeyType, CODEC_VERSION};

fn hex_sort_key(value: i64) -> String {
    let bytes = encode_sort_key_json(SortKeyType::Int8, &serde_json::json!(value)).unwrap();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn index_bound(column_id: i16, min: i64, max: i64) -> CatalogSegmentIndexBound {
    CatalogSegmentIndexBound {
        column_id,
        type_oid: 20,
        codec_version: CODEC_VERSION,
        min_value: hex_sort_key(min),
        max_value: hex_sort_key(max),
    }
}

#[test]
fn catalog_rows_assemble_shared_manifest_with_pk_filter_and_relative_paths() {
    let rows = vec![CatalogManifestSegmentRow {
        path: "001/segment-0001-aaaaaaaa.parquet".to_string(),
        batch_number: 1,
        min_seq: 1,
        max_seq: 10,
        min_commit_seq: 1,
        max_commit_seq: 10,
        row_count: 10,
        byte_size: 128,
        schema_version: 2,
        index_bounds: vec![index_bound(1, 1, 10)],
    }];

    let manifest = manifest_from_catalog_rows(
        "app",
        "items",
        2,
        &[
            ColumnRef::new(ColumnId::from_attnum(7), "id"),
            ColumnRef::new(ColumnId::from_attnum(11), "tenant"),
        ],
        rows,
    )
    .unwrap();

    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(
        manifest.segments[0].path,
        "001/segment-0001-aaaaaaaa.parquet"
    );
    assert_eq!(manifest.max_seq, 10);
    assert_eq!(
        manifest.segments[0].column_stats["1"].min,
        serde_json::json!(1)
    );
    assert_eq!(
        manifest.segments[0].column_stats["1"].max,
        serde_json::json!(10)
    );
    assert_eq!(
        manifest.segments[0]
            .pk_filter
            .as_ref()
            .map(|filter| filter.column_ids.clone()),
        Some(vec![7, 11])
    );
    assert_eq!(
        manifest.segments[0].bloom_filters[0].column_ids,
        vec![7, 11]
    );
    assert_eq!(
        manifest_relative_segment_path(
            "app",
            "items",
            "app/items/001/segment-0002-bbbbbbbb.parquet"
        ),
        "001/segment-0002-bbbbbbbb.parquet"
    );
}

#[test]
fn manifest_paths_and_sharded_round_trip_io() {
    let dir = tempfile::tempdir().unwrap();
    let (relative, absolute) = manifest_paths("app", "notes", dir.path().to_str().unwrap());
    assert_eq!(relative, "app/notes/manifest.json");

    let mut manifest = Manifest::new_shared("app", "notes", 1);
    let segment = build_manifest_segment_from_catalog_row(
        "app",
        "notes",
        &[ColumnRef::new(ColumnId::from_attnum(7), "id")],
        CatalogManifestSegmentRow {
            path: "001/segment-0001-aaaaaaaa.parquet".to_string(),
            batch_number: 1,
            min_seq: 5,
            max_seq: 5,
            min_commit_seq: 5,
            max_commit_seq: 5,
            row_count: 1,
            byte_size: 32,
            schema_version: 1,
            index_bounds: vec![],
        },
    )
    .unwrap();
    manifest.append_segment(segment);

    write_manifest_to_path(&absolute, &manifest).unwrap();
    let loaded = try_load_manifest_from_path(&absolute)
        .expect("manifest load should succeed")
        .expect("manifest should exist");
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
            path: "001/segment-0001-aaaaaaaa.parquet".to_string(),
            batch_number: 1,
            min_seq: 1,
            max_seq: 10,
            min_commit_seq: 1,
            max_commit_seq: 10,
            row_count: 10,
            byte_size: 128,
            schema_version: 1,
            index_bounds: vec![],
        },
        CatalogManifestSegmentRow {
            path: "001/segment-0002-bbbbbbbb.parquet".to_string(),
            batch_number: 2,
            min_seq: 11,
            max_seq: 20,
            min_commit_seq: 11,
            max_commit_seq: 20,
            row_count: 10,
            byte_size: 256,
            schema_version: 1,
            index_bounds: vec![],
        },
    ];
    let manifest = manifest_from_catalog_rows(
        "app",
        "items",
        1,
        &[ColumnRef::new(ColumnId::from_attnum(7), "id")],
        rows,
    )
    .unwrap();
    assert_eq!(manifest.segments.len(), 2);
    assert_eq!(
        manifest.segments[0].path,
        "001/segment-0001-aaaaaaaa.parquet"
    );
    assert_eq!(
        manifest.segments[1].path,
        "001/segment-0002-bbbbbbbb.parquet"
    );
    assert_eq!(manifest.max_seq, 20);
    assert_eq!(manifest.max_commit_seq, 20);
}
