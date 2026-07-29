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
        min_value: Some(hex_sort_key(min)),
        max_value: Some(hex_sort_key(max)),
        row_group_min_values: vec![Some(hex_sort_key(min))],
        row_group_max_values: vec![Some(hex_sort_key(max))],
        row_group_null_counts: vec![Some(0)],
    }
}

#[allow(clippy::too_many_arguments)]
fn catalog_row(
    path: &str,
    batch_number: i32,
    min_seq: i64,
    max_seq: i64,
    row_count: i64,
    byte_size: i64,
    schema_version: i32,
    index_bounds: Vec<CatalogSegmentIndexBound>,
) -> CatalogManifestSegmentRow {
    CatalogManifestSegmentRow {
        segment_id: format!("00000000-0000-0000-0000-{batch_number:012}"),
        path: path.to_string(),
        batch_number,
        min_seq,
        max_seq,
        min_commit_seq: min_seq,
        max_commit_seq: max_seq,
        row_count,
        byte_size,
        schema_version,
        row_group_count: 1,
        row_group_row_counts: vec![row_count],
        row_group_min_seqs: vec![min_seq],
        row_group_max_seqs: vec![max_seq],
        status: "active".to_string(),
        checksum: "a".repeat(64),
        object_etag: None,
        created_at: None,
        index_bounds,
    }
}

#[test]
fn catalog_rows_assemble_shared_manifest_with_pk_filter_and_relative_paths() {
    let rows = vec![catalog_row(
        "001/segment-0001-aaaaaaaa.parquet",
        1,
        1,
        10,
        10,
        128,
        2,
        vec![index_bound(1, 1, 10)],
    )];

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
        manifest.segments[0].column_indexes[0].min_value,
        Some(hex_sort_key(1))
    );
    assert_eq!(
        manifest.segments[0].column_indexes[0].max_value,
        Some(hex_sort_key(10))
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
        &[ColumnRef::new(ColumnId::from_attnum(7), "id")],
        catalog_row(
            "001/segment-0001-aaaaaaaa.parquet",
            1,
            5,
            5,
            1,
            32,
            1,
            vec![],
        ),
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
        catalog_row(
            "001/segment-0001-aaaaaaaa.parquet",
            1,
            1,
            10,
            10,
            128,
            1,
            vec![],
        ),
        catalog_row(
            "001/segment-0002-bbbbbbbb.parquet",
            2,
            11,
            20,
            10,
            256,
            1,
            vec![],
        ),
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
