use std::collections::BTreeMap;

use koldstore_common::{CommitSeq, SeqId};
use koldstore_parquet::{
    ColumnStats, FooterSummary, ParquetReadOptions, RowGroupPruner, RowGroupStats,
};
use serde_json::json;

#[test]
fn reader_options_capture_projection_seq_range_and_pk_values() {
    let options = ParquetReadOptions::new()
        .with_columns(["id", "seq"])
        .with_row_groups([1, 3])
        .with_clean_seq_range(SeqId::new(10).unwrap(), SeqId::new(20).unwrap())
        .with_commit_seq_range(
            "commit_seq",
            CommitSeq::new(11).unwrap(),
            CommitSeq::new(21).unwrap(),
        )
        .with_pk_values("id", ["42"]);

    assert_eq!(options.columns, vec!["id", "seq"]);
    assert_eq!(options.row_groups.as_ref().unwrap(), &vec![1, 3]);
    assert_eq!(options.seq_range.as_ref().unwrap().min.get(), 10);
    assert_eq!(options.commit_seq_range.as_ref().unwrap().max.get(), 21);
    assert_eq!(options.pk_values.as_ref().unwrap().values, vec!["42"]);
}

#[test]
fn reader_options_capture_clean_schema_metadata_projection_and_seq_cursor() {
    let options = ParquetReadOptions::new()
        .with_clean_change_metadata()
        .with_seq_range("seq", SeqId::new(10).unwrap(), SeqId::new(20).unwrap());

    assert_eq!(
        options.columns,
        vec!["seq", "op", "deleted", "schema_version"]
    );
    assert_eq!(options.seq_range.as_ref().unwrap().column, "seq");
    assert!(options.commit_seq_range.is_none());
}

#[test]
fn reader_request_builds_direct_object_store_projection_and_row_group_selection() {
    let request = koldstore_parquet::ParquetReadRequest::new(
        "s3://bucket/app/items/batch-1.parquet",
        ParquetReadOptions::new()
            .with_columns(["id", "status"])
            .with_row_groups([0, 2])
            .with_pk_values("id", ["42"]),
    );

    assert_eq!(request.object_path, "s3://bucket/app/items/batch-1.parquet");
    assert_eq!(request.options.columns, vec!["id", "status"]);
    assert_eq!(request.options.row_groups, Some(vec![0, 2]));
    assert!(request.uses_footer_before_columns());
    assert!(request.uses_pk_bloom_checks());
}

#[test]
fn row_group_pruner_skips_non_overlapping_seq_ranges() {
    let footer = FooterSummary {
        row_groups: vec![
            RowGroupStats {
                row_group: 0,
                min_seq: Some(1),
                max_seq: Some(9),
                min_commit_seq: Some(1),
                max_commit_seq: Some(9),
            },
            RowGroupStats {
                row_group: 1,
                min_seq: Some(10),
                max_seq: Some(20),
                min_commit_seq: Some(10),
                max_commit_seq: Some(20),
            },
        ],
    };

    let decision =
        RowGroupPruner.prune_seq_range(&footer, SeqId::new(10).unwrap(), SeqId::new(20).unwrap());

    assert_eq!(decision.selected_row_groups, vec![1]);
    assert_eq!(decision.skipped_row_groups, 1);
}

#[test]
fn row_group_pruner_skips_non_overlapping_commit_seq_ranges() {
    let footer = FooterSummary {
        row_groups: vec![
            RowGroupStats {
                row_group: 0,
                min_seq: Some(1),
                max_seq: Some(9),
                min_commit_seq: Some(1),
                max_commit_seq: Some(9),
            },
            RowGroupStats {
                row_group: 1,
                min_seq: Some(10),
                max_seq: Some(20),
                min_commit_seq: Some(10),
                max_commit_seq: Some(20),
            },
        ],
    };

    let decision = RowGroupPruner.prune_commit_seq_range(
        &footer,
        CommitSeq::new(10).unwrap(),
        CommitSeq::new(20).unwrap(),
    );

    assert_eq!(decision.selected_row_groups, vec![1]);
    assert_eq!(decision.skipped_row_groups, 1);
}

#[test]
fn row_group_pruner_uses_pk_bloom_may_contain_metadata() {
    let footer = FooterSummary {
        row_groups: vec![
            RowGroupStats {
                row_group: 0,
                min_seq: Some(1),
                max_seq: Some(10),
                min_commit_seq: Some(1),
                max_commit_seq: Some(10),
            },
            RowGroupStats {
                row_group: 1,
                min_seq: Some(11),
                max_seq: Some(20),
                min_commit_seq: Some(11),
                max_commit_seq: Some(20),
            },
            RowGroupStats {
                row_group: 2,
                min_seq: Some(21),
                max_seq: Some(30),
                min_commit_seq: Some(21),
                max_commit_seq: Some(30),
            },
        ],
    };
    let bloom_values = BTreeMap::from([
        (0, vec!["1".to_string(), "2".to_string()]),
        (1, vec!["42".to_string()]),
    ]);

    let decision = RowGroupPruner.prune_pk_values(&footer, &bloom_values, ["42"]);

    assert_eq!(decision.selected_row_groups, vec![1, 2]);
    assert_eq!(decision.skipped_row_groups, 1);
}

#[test]
fn segment_column_stats_pruning_skips_only_proven_non_overlaps() {
    let stats = BTreeMap::from([(
        "created_at".to_string(),
        ColumnStats {
            min: json!("2026-01-10"),
            max: json!("2026-01-20"),
        },
    )]);

    assert!(!RowGroupPruner.segment_column_may_overlap(
        &stats,
        "created_at",
        &json!("2026-02-01"),
        &json!("2026-02-28")
    ));
    assert!(RowGroupPruner.segment_column_may_overlap(
        &stats,
        "created_at",
        &json!("2026-01-15"),
        &json!("2026-01-31")
    ));
}

#[test]
fn segment_column_stats_pruning_scans_when_metadata_is_missing_or_incomparable() {
    let stats = BTreeMap::from([(
        "score".to_string(),
        ColumnStats {
            min: json!(10),
            max: json!(20),
        },
    )]);

    assert!(RowGroupPruner.segment_column_may_overlap(&stats, "missing", &json!(30), &json!(40)));
    assert!(RowGroupPruner.segment_column_may_overlap(&stats, "score", &json!("30"), &json!("40")));
}

#[test]
fn pk_point_lookup_prunes_row_groups_via_stats_and_bloom() {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int64Array, RecordBatch, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
    use koldstore_parquet::{
        read_clean_cold_rows_with_options, select_row_groups_for_pk_values, ParquetSegmentWriter,
        PgColumn, PgType, WriterOptions,
    };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pk-prune.parquet");

    // Three row groups of 2 ids each: [1,2], [3,4], [5,6].
    let ids = vec![1_i64, 2, 3, 4, 5, 6];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("seq", DataType::Int64, false),
        Field::new("deleted", DataType::Boolean, false),
        Field::new("schema_version", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(BooleanArray::from(vec![false; ids.len()])),
            Arc::new(UInt32Array::from(vec![1_u32; ids.len()])),
        ],
    )
    .unwrap();

    let writer = ParquetSegmentWriter::new(
        WriterOptions {
            row_group_size: 2,
            ..WriterOptions::default()
        }
        .with_statistics_columns(["id", "seq"])
        .with_bloom_filter_columns(["id"]),
    );
    let file = std::fs::File::create(&path).unwrap();
    let metadata = writer
        .write_record_batches(file, schema, vec![batch])
        .unwrap();
    assert_eq!(metadata.num_row_groups(), 3);

    let decision = select_row_groups_for_pk_values(&path, "id", &["4".to_string()]).unwrap();
    assert_eq!(
        decision.selected_row_groups,
        vec![1],
        "id=4 should keep only the middle row group"
    );
    assert_eq!(decision.skipped_row_groups, 2);

    let columns = vec![PgColumn::new("id", PgType::Int8, false)];
    let rows = read_clean_cold_rows_with_options(
        &path,
        &columns,
        &["id".to_string()],
        &ParquetReadOptions::new()
            .with_columns(["id"])
            .with_pk_values("id", ["4"]),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].pk_json["id"], json!(4));
}

#[test]
fn object_store_pk_point_lookup_uses_footer_first_range_reads() {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int64Array, RecordBatch, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
    use koldstore_parquet::{
        read_clean_cold_rows_from_object_store, ParquetSegmentWriter, PgColumn, PgType,
        WriterOptions,
    };
    use koldstore_storage::{ObjectStoreClient, StorageClient};

    let ids = vec![1_i64, 2, 3, 4, 5, 6];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("seq", DataType::Int64, false),
        Field::new("deleted", DataType::Boolean, false),
        Field::new("schema_version", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(BooleanArray::from(vec![false; ids.len()])),
            Arc::new(UInt32Array::from(vec![1_u32; ids.len()])),
        ],
    )
    .unwrap();

    let writer = ParquetSegmentWriter::new(
        WriterOptions {
            row_group_size: 2,
            ..WriterOptions::default()
        }
        .with_statistics_columns(["id", "seq"])
        .with_bloom_filter_columns(["id"]),
    );
    let mut encoded = Vec::new();
    writer
        .write_record_batches(&mut encoded, schema, vec![batch])
        .unwrap();

    let client = ObjectStoreClient::in_memory();
    let key = "segments/pk-prune.parquet";
    client
        .put(key, &encoded, koldstore_storage::PutPrecondition::Overwrite)
        .unwrap();

    let columns = vec![PgColumn::new("id", PgType::Int8, false)];
    let rows = read_clean_cold_rows_from_object_store(
        client.store(),
        key,
        &columns,
        &["id".to_string()],
        &ParquetReadOptions::new()
            .with_columns(["id"])
            .with_pk_values("id", ["4"]),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].pk_json["id"], json!(4));
}
#[test]
fn object_store_pk_point_lookup_reads_less_than_full_file_via_ranges() {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int64Array, RecordBatch, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
    use koldstore_parquet::{
        read_clean_cold_rows_from_object_store_with_stats, ObjectStoreReadStats,
        ParquetSegmentWriter, PgColumn, PgType, WriterOptions,
    };
    use koldstore_storage::{ObjectStoreClient, StorageClient};

    let ids = vec![1_i64, 2, 3, 4, 5, 6];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("seq", DataType::Int64, false),
        Field::new("deleted", DataType::Boolean, false),
        Field::new("schema_version", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(BooleanArray::from(vec![false; ids.len()])),
            Arc::new(UInt32Array::from(vec![1_u32; ids.len()])),
        ],
    )
    .unwrap();
    let writer = ParquetSegmentWriter::new(
        WriterOptions {
            row_group_size: 2,
            ..WriterOptions::default()
        }
        .with_statistics_columns(["id", "seq"])
        .with_bloom_filter_columns(["id"]),
    );
    let mut encoded = Vec::new();
    writer
        .write_record_batches(&mut encoded, schema, vec![batch])
        .unwrap();
    let file_size = encoded.len() as u64;

    let client = ObjectStoreClient::in_memory();
    let key = "segments/pk-prune-stats.parquet";
    client
        .put(key, &encoded, koldstore_storage::PutPrecondition::Overwrite)
        .unwrap();

    let io = Arc::new(ObjectStoreReadStats::default());
    let columns = vec![PgColumn::new("id", PgType::Int8, false)];
    let rows = read_clean_cold_rows_from_object_store_with_stats(
        client.store(),
        key,
        Some(file_size),
        Some(Arc::clone(&io)),
        &columns,
        &["id".to_string()],
        &ParquetReadOptions::new()
            .with_columns(["id"])
            .with_pk_values("id", ["4"]),
    )
    .unwrap()
    .0;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].pk_json["id"], json!(4));

    let (range_calls, bytes_read) = io.snapshot();
    assert!(
        range_calls >= 1,
        "footer/column data must go through ObjectStore range APIs"
    );
    assert!(
        bytes_read < file_size,
        "range reads ({bytes_read}) must be strictly less than full file ({file_size}); \
         min/max prune should skip other row groups without downloading them"
    );
}

#[test]
fn object_store_read_profile_reports_footer_first_and_bloom_skip() {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int64Array, RecordBatch, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
    use koldstore_parquet::{
        read_clean_cold_rows_from_object_store_with_size, BloomPruneMode, ParquetSegmentWriter,
        PgColumn, PgType, WriterOptions,
    };
    use koldstore_storage::{ObjectStoreClient, StorageClient};

    let ids = vec![1_i64, 2, 3, 4, 5, 6];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("seq", DataType::Int64, false),
        Field::new("deleted", DataType::Boolean, false),
        Field::new("schema_version", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(BooleanArray::from(vec![false; ids.len()])),
            Arc::new(UInt32Array::from(vec![1_u32; ids.len()])),
        ],
    )
    .unwrap();
    let writer = ParquetSegmentWriter::new(
        WriterOptions {
            row_group_size: 2,
            ..WriterOptions::default()
        }
        .with_statistics_columns(["id", "seq"])
        .with_bloom_filter_columns(["id"]),
    );
    let mut encoded = Vec::new();
    writer
        .write_record_batches(&mut encoded, schema, vec![batch])
        .unwrap();
    let file_size = encoded.len() as u64;
    let client = ObjectStoreClient::in_memory();
    let key = "segments/profile.parquet";
    client
        .put(key, &encoded, koldstore_storage::PutPrecondition::Overwrite)
        .unwrap();

    let (_rows, profile) = read_clean_cold_rows_from_object_store_with_size(
        client.store(),
        key,
        Some(file_size),
        &[PgColumn::new("id", PgType::Int8, false)],
        &["id".to_string()],
        &ParquetReadOptions::new()
            .with_columns(["id"])
            .with_pk_values("id", ["4"]),
    )
    .unwrap();

    assert!(profile.footer_first);
    assert_eq!(profile.row_groups_total, 3);
    assert_eq!(profile.row_groups_selected, vec![1]);
    assert_eq!(profile.row_groups_skipped, 2);
    assert!(profile.stats_pruned);
    assert_eq!(profile.bloom, BloomPruneMode::SkippedAfterStats);
    assert_eq!(profile.bloom_filters_fetched, 0);
    assert!(profile.bytes_read < file_size);
    assert!(profile.format_io_summary().contains("footer-first"));
    assert!(profile.format_row_groups_summary().contains("selected=[1]"));
    assert!(profile
        .format_bloom_summary()
        .contains("skipped_after_stats"));
}
