//! Flush segment publish durability: encode → Create publish → readable final.

use koldstore_flush::{write_flush_segment_with_client, FlushStats, FlushWriteChunk};
use koldstore_parquet::{
    plan_clean_cold_record, record_batch_from_clean_cold_records, ColdRecordBatch, PgColumn, PgType,
};
use koldstore_storage::{open_filesystem_client, StorageClient, StorageClientError};
use serde_json::json;

fn cold_chunk(rows: usize) -> FlushWriteChunk {
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
    let batch = record_batch_from_clean_cold_records(
        &[
            PgColumn::new("id", PgType::Int8, false),
            PgColumn::new("body", PgType::Text, true),
        ],
        &plans,
    )
    .unwrap();
    let mut indexed_bounds = std::collections::BTreeMap::new();
    indexed_bounds.insert("id".to_string(), (json!(1), json!(rows)));
    let cold_batch = ColdRecordBatch {
        batch,
        row_count: rows,
        indexed_bounds,
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
    FlushWriteChunk::from_encoded_batches(parquet_bytes, &[cold_batch])
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

    assert!(
        written
            .object_path
            .starts_with(&format!("app/items/batch-0-{}.", written.segment_id)),
        "object path should embed segment_id, got {}",
        written.object_path
    );
    assert!(written.object_path.ends_with(".parquet"));
    assert!(written.byte_size > 0);
    assert_eq!(written.checksum.len(), 64);
    let bytes = client.get(&written.object_path).unwrap();
    assert_eq!(bytes.len() as i64, written.byte_size);
    assert_eq!(
        written.checksum,
        koldstore_storage::content_checksum_sha256_hex(&bytes)
    );
    koldstore_parquet::validate_parquet_bytes(&bytes).unwrap();

    // Same payload republished through the storage layer reuses the final key.
    let temp = koldstore_storage::temp_object_key(
        "app/items",
        "retry-writer",
        &koldstore_storage::unique_temp_file_name("batch-0.parquet"),
    );
    let published = koldstore_storage::publish_immutable_object(
        &client,
        &temp,
        &written.object_path,
        &chunk.parquet_bytes,
    )
    .unwrap();
    assert!(published.reused_existing);
    assert_eq!(published.byte_size, written.byte_size as u64);

    let temps = client.list("app/items/.tmp").unwrap();
    assert!(temps.is_empty(), "leftover temps: {temps:?}");
}

#[test]
fn flush_segment_retry_after_orphan_uses_new_object_key() {
    let root = tempfile::tempdir().unwrap();
    let client = open_filesystem_client(root.path().to_str().unwrap()).unwrap();

    // Simulate a rolled-back flush that left an unreferenced final object for
    // batch_number=1 while concurrent DML changed the next encode payload.
    let orphan = cold_chunk(3);
    let orphan_stats = FlushStats::from_write_chunk(&orphan).unwrap();
    let first = write_flush_segment_with_client(
        &client,
        "app",
        "items",
        "zstd",
        &["id".to_string()],
        &["id".to_string()],
        1,
        1,
        &orphan,
        &orphan_stats,
    )
    .unwrap();

    let retry_chunk = cold_chunk(5);
    let retry_stats = FlushStats::from_write_chunk(&retry_chunk).unwrap();
    let second = write_flush_segment_with_client(
        &client,
        "app",
        "items",
        "zstd",
        &["id".to_string()],
        &["id".to_string()],
        1,
        1, // same batch_number as the orphaned attempt
        &retry_chunk,
        &retry_stats,
    )
    .unwrap();

    assert_ne!(
        first.object_path, second.object_path,
        "retry must not collide with orphaned final object"
    );
    assert_ne!(first.segment_id, second.segment_id);
    assert_ne!(first.byte_size, second.byte_size);
    assert_eq!(
        client.get(&first.object_path).unwrap().len() as i64,
        first.byte_size
    );
    assert_eq!(
        client.get(&second.object_path).unwrap().len() as i64,
        second.byte_size
    );
}

#[test]
fn flush_segment_publish_rejects_corrupt_existing_final() {
    let root = tempfile::tempdir().unwrap();
    let client = open_filesystem_client(root.path().to_str().unwrap()).unwrap();
    let segment_id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let final_key = format!("app/items/batch-1-{segment_id}.parquet");
    client
        .put(
            &final_key,
            b"not-parquet",
            koldstore_storage::PutPrecondition::CreateIfAbsent,
        )
        .unwrap();

    let err = koldstore_storage::publish_immutable_object(
        &client,
        "app/items/.tmp/w/corrupt.tmp",
        &final_key,
        b"different-payload-bytes",
    )
    .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("expected")
            || message.contains("Validation")
            || message.contains("size")
            || message.contains("content"),
        "unexpected error: {message}"
    );
    assert_eq!(client.get(&final_key).unwrap(), b"not-parquet");
}

#[test]
fn missing_final_returns_not_found_from_storage_client() {
    let client = koldstore_storage::ObjectStoreClient::in_memory();
    assert!(matches!(
        client.get("no/such/object"),
        Err(StorageClientError::NotFound { .. })
    ));
}
