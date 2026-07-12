//! Flush segment publish durability: encode → Create publish → readable final.

use koldstore_common::ColumnId;
use koldstore_flush::{write_flush_segment_with_client, FlushStats, FlushWriteChunk};
use koldstore_parquet::{
    plan_clean_cold_record, record_batch_from_clean_cold_records, ColdRecordBatch, PgColumn, PgType,
};
use koldstore_storage::{open_filesystem_client, StorageClient, StorageClientError};
use serde_json::json;

fn cold_chunk(rows: usize) -> FlushWriteChunk {
    let columns = [
        PgColumn::new(ColumnId::new(1).unwrap(), "id", PgType::Int8, false),
        PgColumn::new(ColumnId::new(2).unwrap(), "body", PgType::Text, true),
    ];
    let plans: Vec<_> = (1..=rows as i64)
        .map(|id| {
            plan_clean_cold_record(
                [
                    ("id", json!(id)),
                    ("body", json!(if id % 2 == 0 { "even" } else { "odd" })),
                ],
                ["id"],
                id,
                1,
                1,
            )
            .unwrap()
        })
        .collect();
    let batch = record_batch_from_clean_cold_records(&columns, &plans).unwrap();
    let cold_batch = ColdRecordBatch {
        batch,
        row_count: rows,
        min_seq: 1,
        max_seq: rows as i64,
    };
    let parquet_bytes = koldstore_parquet::encode_parquet_segment_bytes(
        &cold_batch.batch,
        &["id".to_string()],
        &["id".to_string()],
        "zstd",
    )
    .unwrap();
    FlushWriteChunk::from_encoded_batches(parquet_bytes, &[cold_batch], &columns[..1]).unwrap()
}

#[test]
fn flush_segment_publish_create_is_readable_and_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let client = open_filesystem_client(root.path().to_str().unwrap()).unwrap();
    let chunk = cold_chunk(5);
    let stats = FlushStats::from_write_chunk(&chunk).unwrap();

    let written = write_flush_segment_with_client(
        &client,
        "app",
        "items",
        "zstd",
        &["id".to_string()],
        &["id".to_string()],
        1,
        0,
        &chunk,
        &stats,
    )
    .unwrap();

    assert_eq!(written.object_path, "app/items/segment-0000.parquet");
    assert!(written.byte_size > 0);
    let bytes = client.get(&written.object_path).unwrap();
    assert_eq!(bytes.len() as i64, written.byte_size);
    koldstore_parquet::validate_parquet_bytes(&bytes).unwrap();

    let written2 = write_flush_segment_with_client(
        &client,
        "app",
        "items",
        "zstd",
        &["id".to_string()],
        &["id".to_string()],
        1,
        0,
        &chunk,
        &stats,
    )
    .unwrap();
    assert_eq!(written2.object_path, written.object_path);
    assert_eq!(written2.byte_size, written.byte_size);

    let temps = client.list("app/items/.tmp").unwrap();
    assert!(temps.is_empty(), "leftover temps: {temps:?}");
}

#[test]
fn flush_segment_publish_rejects_corrupt_existing_final() {
    let root = tempfile::tempdir().unwrap();
    let client = open_filesystem_client(root.path().to_str().unwrap()).unwrap();
    client
        .put(
            "app/items/segment-0001.parquet",
            b"not-parquet",
            koldstore_storage::PutPrecondition::CreateIfAbsent,
        )
        .unwrap();

    let chunk = cold_chunk(3);
    let stats = FlushStats::from_write_chunk(&chunk).unwrap();
    let err = write_flush_segment_with_client(
        &client,
        "app",
        "items",
        "zstd",
        &["id".to_string()],
        &["id".to_string()],
        1,
        1,
        &chunk,
        &stats,
    )
    .unwrap_err();
    assert!(
        err.contains("expected") || err.contains("Validation") || err.contains("size"),
        "unexpected error: {err}"
    );
    assert_eq!(
        client.get("app/items/segment-0001.parquet").unwrap(),
        b"not-parquet"
    );
}

#[test]
fn missing_final_returns_not_found_from_storage_client() {
    let client = koldstore_storage::ObjectStoreClient::in_memory();
    assert!(matches!(
        client.get("no/such/object"),
        Err(StorageClientError::NotFound { .. })
    ));
}
