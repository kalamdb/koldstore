use koldstore_parquet::{
    plan_clean_cold_record, record_batch_from_clean_cold_records, ColumnStats, FooterSummary,
    ParquetSegmentWriter, PgColumn, PgType, RowGroupStats, SegmentFooterMetadata,
    SegmentMetadataInput, SegmentSplitPolicy, StreamingParquetSegmentWriter, WriterOptions,
};
use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema};
use serde_json::json;

#[test]
fn writer_plan_records_kalamdb_compatible_layout_metadata() {
    let writer = ParquetSegmentWriter::new(WriterOptions::default());
    let plan = writer.plan_segment("app/items", 7, 1, 10, 11, 20);

    assert_eq!(plan.object_path, "app/items/001/segment-0007.parquet");
    assert_eq!(plan.min_seq, 1);
    assert_eq!(plan.max_seq, 10);
    assert_eq!(plan.min_commit_seq, 11);
    assert_eq!(plan.max_commit_seq, 20);
    assert_eq!(plan.compression, "zstd");
}

#[test]
fn clean_cold_record_plan_writes_live_rows_and_pk_only_delete_markers() {
    let live = plan_clean_cold_record(
        [("id", json!(1)), ("body", json!("hello"))],
        ["id"],
        10,
        2,
        1,
    )
    .unwrap();
    assert!(!live.deleted);
    assert_eq!(live.values["id"], json!(1));
    assert_eq!(live.values["body"], json!("hello"));
    assert_eq!(live.values["seq"], json!(10));
    assert_eq!(live.values["op"], json!(2));
    assert_eq!(live.values["schema_version"], json!(1));

    let tombstone = plan_clean_cold_record(
        [("id", json!(1)), ("body", json!("stale-payload"))],
        ["id"],
        11,
        3,
        1,
    )
    .unwrap();
    assert!(tombstone.deleted);
    assert_eq!(tombstone.values["id"], json!(1));
    assert!(!tombstone.values.contains_key("body"));
    assert_eq!(tombstone.values["seq"], json!(11));
    assert_eq!(tombstone.values["op"], json!(3));
    assert_eq!(tombstone.values["deleted"], json!(true));
}

#[test]
fn writer_plan_records_stats_and_pk_bloom_metadata_for_manifest_round_trip() {
    let writer = ParquetSegmentWriter::new(WriterOptions::default());
    let plan = writer.plan_segment_with_metadata(
        "app/items",
        7,
        SegmentMetadataInput {
            min_seq: 1,
            max_seq: 10,
            min_commit_seq: 11,
            max_commit_seq: 20,
            row_count: 100,
            byte_size: 4096,
            pk_columns: vec!["id".to_string()],
            bloom_filter_columns: vec!["id".to_string(), "created_at".to_string()],
            statistics_columns: vec!["id".to_string(), "created_at".to_string()],
            column_stats: vec![
                (
                    "id".to_string(),
                    ColumnStats {
                        min: json!(1),
                        max: json!(100),
                    },
                ),
                (
                    "seq".to_string(),
                    ColumnStats {
                        min: json!(1),
                        max: json!(10),
                    },
                ),
                (
                    "commit_seq".to_string(),
                    ColumnStats {
                        min: json!(11),
                        max: json!(20),
                    },
                ),
            ],
        },
    );

    assert_eq!(plan.object_path, "app/items/001/segment-0007.parquet");
    assert_eq!(plan.row_count, 100);
    assert_eq!(plan.byte_size, 4096);
    assert_eq!(plan.pk_filter_kind.as_deref(), Some("bloom"));
    assert_eq!(plan.pk_filter_columns, vec!["id"]);
    assert_eq!(plan.bloom_filter_columns, vec!["id", "created_at"]);
    assert_eq!(plan.statistics_columns, vec!["id", "created_at"]);
    assert!(plan.writes_native_bloom_filters);
    assert_eq!(
        plan.column_stats
            .get("seq")
            .map(|stats| (&stats.min, &stats.max)),
        Some((&json!(1), &json!(10)))
    );
    assert!(plan.column_stats.contains_key("id"));
    assert!(plan.column_stats.contains_key("commit_seq"));
}

#[test]
fn writer_plan_bounds_streaming_row_groups_by_configured_row_group_size() {
    let writer = ParquetSegmentWriter::new(WriterOptions {
        compression: "zstd".to_string(),
        row_group_size: 2,
        statistics_columns: Vec::new(),
        bloom_filter_columns: Vec::new(),
        bloom_filter_false_positive_rate: Some(0.01),
    });

    let plan = writer.plan_streaming_row_groups(5);

    assert_eq!(plan.row_group_count, 3);
    assert_eq!(plan.max_rows_in_memory, 2);
    assert_eq!(plan.total_rows, 5);
}

#[test]
fn segment_split_policy_closes_at_compressed_byte_boundary() {
    let policy = SegmentSplitPolicy::new(Some(1_024), 10_000);

    assert!(!policy.should_close(1_023, 999));
    assert!(policy.should_close(1_024, 999));
    assert!(policy.should_close(1_025, 999));
}

#[test]
fn segment_split_policy_closes_at_row_cap_before_size_target() {
    let policy = SegmentSplitPolicy::new(Some(1_024), 3);

    assert!(!policy.should_close(100, 2));
    assert!(policy.should_close(100, 3));
}

#[test]
fn segment_split_policy_without_size_target_uses_only_row_cap() {
    let policy = SegmentSplitPolicy::new(None, 3);

    assert!(!policy.should_close(u64::MAX, 2));
    assert!(policy.should_close(0, 3));
}

#[test]
fn streaming_segment_writer_reports_compressed_bytes_before_close() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut writer =
        StreamingParquetSegmentWriter::try_new(schema, WriterOptions::default()).unwrap();

    writer.write_batch(&batch).unwrap();

    assert!(writer.current_bytes() > 0);
    let bytes = writer.finish().unwrap();
    let validation = koldstore_parquet::validate_parquet_bytes(&bytes).unwrap();
    assert_eq!(validation.row_count, 3);
}

#[test]
fn writer_options_build_native_parquet_properties_for_stats_and_bloom_filters() {
    let options = WriterOptions::default()
        .with_statistics_columns(["id", "created_at"])
        .with_bloom_filter_columns(["id"]);
    let properties = options.try_native_writer_properties().unwrap();
    let id = parquet::schema::types::ColumnPath::from("id");
    let created_at = parquet::schema::types::ColumnPath::from("created_at");

    assert_ne!(
        properties.statistics_enabled(&id),
        parquet::file::properties::EnabledStatistics::None
    );
    assert_eq!(
        properties.max_row_group_row_count(),
        Some(WriterOptions::default().row_group_size)
    );
    assert!(properties.bloom_filter_properties(&id).is_some());
    assert!(properties.bloom_filter_properties(&created_at).is_none());
}

#[test]
fn writer_options_apply_configured_compression_codec() {
    let properties = WriterOptions {
        compression: "zstd".to_string(),
        ..WriterOptions::default()
    }
    .try_native_writer_properties()
    .unwrap();

    let id = parquet::schema::types::ColumnPath::from("id");
    assert_eq!(
        properties.compression(&id),
        parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default())
    );
}

#[test]
fn writer_options_reject_unknown_compression_codec() {
    let result = WriterOptions {
        compression: "made_up_codec".to_string(),
        ..WriterOptions::default()
    }
    .try_native_writer_properties();

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .contains("unsupported parquet compression"));
}

#[test]
fn clean_cold_record_batch_builder_preserves_payloads_and_metadata_types() {
    let rows = vec![
        plan_clean_cold_record(
            [
                ("id", json!(1)),
                ("amount", json!(123.4500)),
                ("tags", json!(["alpha", "beta"])),
                ("binary_hash", json!("\\xdeadbeef")),
                ("created_at", json!("2026-07-06T00:00:00Z")),
            ],
            ["id"],
            10,
            1,
            7,
        )
        .unwrap(),
        plan_clean_cold_record(
            [
                ("id", json!(2)),
                ("amount", json!(null)),
                ("tags", json!(["gamma"])),
                ("binary_hash", json!(null)),
                ("created_at", json!("2026-07-06T00:00:02Z")),
            ],
            ["id"],
            11,
            3,
            7,
        )
        .unwrap(),
    ];
    let batch = record_batch_from_clean_cold_records(
        &[
            PgColumn::new("id", PgType::Int8, false),
            PgColumn::new("amount", PgType::Numeric, true),
            PgColumn::new("tags", PgType::TextArray, true),
            PgColumn::new("binary_hash", PgType::Bytea, true),
            PgColumn::new("created_at", PgType::Timestamptz, true),
        ],
        &rows,
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 2);
    let id = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.value(0), 1);
    assert_eq!(id.value(1), 2);

    let amount = batch
        .column_by_name("amount")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(amount.value(0), "123.45");
    assert!(amount.is_null(1));

    let tags = batch
        .column_by_name("tags")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(tags.value(0), r#"["alpha","beta"]"#);
    assert!(tags.is_null(1));

    let binary_hash = batch
        .column_by_name("binary_hash")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(binary_hash.value(0), "\\xdeadbeef");
    assert!(binary_hash.is_null(1));

    let created_at = batch
        .column_by_name("created_at")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let expected_created_at = chrono::DateTime::parse_from_rfc3339("2026-07-06T00:00:00Z")
        .unwrap()
        .timestamp_micros();
    assert_eq!(created_at.value(0), expected_created_at);

    let deleted = batch
        .column_by_name("deleted")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!deleted.value(0));
    assert!(deleted.value(1));
}

#[test]
fn writer_writes_record_batches_with_native_parquet_properties() {
    let writer = ParquetSegmentWriter::new(
        WriterOptions::default()
            .with_statistics_columns(["id", "status"])
            .with_bloom_filter_columns(["id"]),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["new", "done"])),
        ],
    )
    .unwrap();

    let metadata = writer
        .write_record_batches(Vec::new(), schema, vec![batch])
        .unwrap();

    assert_eq!(metadata.file_metadata().num_rows(), 2);
    assert_eq!(metadata.num_row_groups(), 1);
}

#[test]
fn footer_summary_extracts_segment_pruning_metadata_for_manifest() {
    let footer = FooterSummary {
        row_groups: vec![
            RowGroupStats {
                row_group: 0,
                min_seq: Some(1),
                max_seq: Some(10),
                min_commit_seq: Some(11),
                max_commit_seq: Some(20),
            },
            RowGroupStats {
                row_group: 1,
                min_seq: Some(11),
                max_seq: Some(30),
                min_commit_seq: Some(21),
                max_commit_seq: Some(40),
            },
        ],
    };

    let metadata = SegmentFooterMetadata::from_footer(
        &footer,
        250,
        8192,
        3,
        vec![(
            "id".to_string(),
            ColumnStats {
                min: json!(1),
                max: json!(250),
            },
        )],
    )
    .unwrap();

    assert_eq!(metadata.min_seq, 1);
    assert_eq!(metadata.max_seq, 30);
    assert_eq!(metadata.min_commit_seq, 11);
    assert_eq!(metadata.max_commit_seq, 40);
    assert_eq!(metadata.row_count, 250);
    assert_eq!(metadata.byte_size, 8192);
    assert_eq!(metadata.schema_version, 3);
    assert!(metadata.column_stats.contains_key("id"));
}
