//! Deep Parquet encode / validate / roundtrip combination tests.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use koldstore_parquet::{
    encode_parquet_segment_bytes, plan_clean_cold_record, read_clean_cold_rows_from_object_store,
    record_batch_from_clean_cold_records, validate_parquet_bytes, ParquetReadOptions,
    ParquetSegmentWriter, PgColumn, PgType, WriterOptions,
};
use koldstore_storage::{
    publish_immutable_object, temp_object_key, ObjectStoreClient, StorageClient,
};
use serde_json::json;

fn sample_batch(rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).collect();
    let bodies: Vec<String> = (0..rows).map(|i| format!("body-{i}")).collect();
    let plans: Vec<_> = ids
        .iter()
        .zip(bodies.iter())
        .enumerate()
        .map(|(i, (id, body))| {
            plan_clean_cold_record(
                [("id", json!(id)), ("body", json!(body))],
                ["id"],
                (i as i64) + 1,
                1,
                1,
            )
            .unwrap()
        })
        .collect();
    record_batch_from_clean_cold_records(
        &[
            PgColumn::new("id", PgType::Int8, false),
            PgColumn::new("body", PgType::Text, false),
        ],
        &plans,
    )
    .unwrap()
}

#[test]
fn encode_validate_roundtrip_across_compression_and_row_group_sizes() {
    let compressions = ["zstd", "snappy", "uncompressed"];
    let row_group_sizes = [1usize, 2, 7, 1024];
    let row_counts = [0usize, 1, 3, 17];

    for &compression in &compressions {
        for &rg in &row_group_sizes {
            for &rows in &row_counts {
                if rows == 0 {
                    // Empty batch still must produce a valid footer.
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("id", DataType::Int64, false),
                        Field::new("body", DataType::Utf8, false),
                    ]));
                    let batch = RecordBatch::new_empty(schema);
                    let writer = ParquetSegmentWriter::new(WriterOptions {
                        compression: compression.to_string(),
                        row_group_size: rg,
                        statistics_columns: vec!["id".to_string()],
                        bloom_filter_columns: vec!["id".to_string()],
                        bloom_filter_false_positive_rate: Some(0.01),
                    });
                    let bytes = writer.encode_record_batch(&batch).unwrap();
                    let validation = validate_parquet_bytes(&bytes).unwrap();
                    assert_eq!(validation.row_count, 0);
                    assert_eq!(validation.byte_size, bytes.len() as u64);
                    continue;
                }

                let batch = sample_batch(rows);
                let pk = vec!["id".to_string()];
                let indexed = vec!["id".to_string()];
                // encode_parquet_segment_bytes uses default WriterOptions row_group_size;
                // exercise custom options via the writer directly too.
                let writer = ParquetSegmentWriter::new(
                    WriterOptions {
                        compression: compression.to_string(),
                        row_group_size: rg,
                        ..WriterOptions::default()
                    }
                    .with_statistics_columns(["id", "seq"])
                    .with_bloom_filter_columns(["id"]),
                );
                let bytes = writer.encode_record_batch(&batch).unwrap();
                let validation = validate_parquet_bytes(&bytes).unwrap();
                assert_eq!(validation.row_count, rows as u64, "c={compression} rg={rg}");
                let expected_groups = rows.div_ceil(rg);
                assert_eq!(
                    validation.row_group_count, expected_groups,
                    "c={compression} rg={rg} rows={rows}"
                );

                let default_bytes =
                    encode_parquet_segment_bytes(&batch, &pk, &indexed, compression).unwrap();
                assert!(validate_parquet_bytes(&default_bytes).is_ok());
            }
        }
    }
}

#[test]
fn validate_parquet_bytes_rejects_truncated_and_corrupt_payloads() {
    let batch = sample_batch(5);
    let bytes =
        encode_parquet_segment_bytes(&batch, &["id".to_string()], &["id".to_string()], "zstd")
            .unwrap();

    assert!(validate_parquet_bytes(&bytes[..4]).is_err());
    assert!(validate_parquet_bytes(&bytes[..bytes.len() / 2]).is_err());

    let mut bad_magic = bytes.clone();
    bad_magic[0] = b'X';
    assert!(validate_parquet_bytes(&bad_magic).is_err());

    let mut bad_footer = bytes.clone();
    let last = bad_footer.len() - 1;
    bad_footer[last] = b'X';
    assert!(validate_parquet_bytes(&bad_footer).is_err());

    assert!(validate_parquet_bytes(b"").is_err());
    assert!(validate_parquet_bytes(b"PAR1").is_err());
}

#[test]
fn encoded_parquet_is_readable_after_in_memory_immutable_publish() {
    let batch = sample_batch(11);
    let bytes =
        encode_parquet_segment_bytes(&batch, &["id".to_string()], &["id".to_string()], "zstd")
            .unwrap();
    validate_parquet_bytes(&bytes).unwrap();
    let client = ObjectStoreClient::in_memory();
    let final_key = "app/items/batch-0.parquet";
    publish_immutable_object(
        &client,
        &temp_object_key("app/items", "writer", "batch-0.parquet.tmp"),
        final_key,
        &bytes,
    )
    .unwrap();
    let published = client.get(final_key).unwrap();
    assert_eq!(published.len(), bytes.len());
    let rows = read_clean_cold_rows_from_object_store(
        client.store(),
        final_key,
        &[
            PgColumn::new("id", PgType::Int8, false),
            PgColumn::new("body", PgType::Text, false),
        ],
        &["id".to_string()],
        &ParquetReadOptions::default(),
    )
    .unwrap();
    assert_eq!(rows.len(), 11);
    assert_eq!(rows[0].seq, 1);
    assert_eq!(rows[10].seq, 11);
}

#[test]
fn writer_properties_set_bloom_max_ndv_to_row_group_size() {
    let options = WriterOptions {
        row_group_size: 77,
        ..WriterOptions::default()
    }
    .with_bloom_filter_columns(["id"]);
    let props = options.try_native_writer_properties().unwrap();
    let id = parquet::schema::types::ColumnPath::from("id");
    let bloom = props.bloom_filter_properties(&id).unwrap();
    assert_eq!(bloom.ndv(), 77);
}

#[test]
fn large_batch_splits_into_configured_row_groups_without_manual_flush() {
    let batch = sample_batch(10);
    let writer = ParquetSegmentWriter::new(WriterOptions {
        compression: "zstd".to_string(),
        row_group_size: 3,
        statistics_columns: vec!["id".to_string()],
        bloom_filter_columns: vec!["id".to_string()],
        bloom_filter_false_positive_rate: Some(0.01),
    });
    let bytes = writer.encode_record_batch(&batch).unwrap();
    let validation = validate_parquet_bytes(&bytes).unwrap();
    assert_eq!(validation.row_count, 10);
    assert_eq!(validation.row_group_count, 4); // 3+3+3+1
}

#[test]
fn simple_arrow_batch_roundtrip_through_encode() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let writer = ParquetSegmentWriter::new(
        WriterOptions::default()
            .with_statistics_columns(["id", "status"])
            .with_bloom_filter_columns(["id"]),
    );
    let bytes = writer.encode_record_batch(&batch).unwrap();
    let validation = validate_parquet_bytes(&bytes).unwrap();
    assert_eq!(validation.row_count, 3);
    assert_eq!(validation.row_group_count, 1);
}
