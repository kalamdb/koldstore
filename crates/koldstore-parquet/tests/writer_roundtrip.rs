use koldstore_parquet::{
    ColumnStats, FooterSummary, ParquetSegmentWriter, RowGroupStats, SegmentFooterMetadata,
    SegmentMetadataInput, WriterOptions,
};
use serde_json::json;

#[test]
fn writer_plan_records_kalamdb_compatible_layout_metadata() {
    let writer = ParquetSegmentWriter::new(WriterOptions::default());
    let plan = writer.plan_segment("app/items", 7, 1, 10, 11, 20);

    assert_eq!(plan.object_path, "app/items/batch-7.parquet");
    assert_eq!(plan.min_seq, 1);
    assert_eq!(plan.max_seq, 10);
    assert_eq!(plan.min_commit_seq, 11);
    assert_eq!(plan.max_commit_seq, 20);
    assert_eq!(plan.compression, "snappy");
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
            column_stats: vec![
                (
                    "id".to_string(),
                    ColumnStats {
                        min: json!(1),
                        max: json!(100),
                    },
                ),
                (
                    "_seq".to_string(),
                    ColumnStats {
                        min: json!(1),
                        max: json!(10),
                    },
                ),
                (
                    "_commit_seq".to_string(),
                    ColumnStats {
                        min: json!(11),
                        max: json!(20),
                    },
                ),
            ],
        },
    );

    assert_eq!(plan.object_path, "app/items/batch-7.parquet");
    assert_eq!(plan.row_count, 100);
    assert_eq!(plan.byte_size, 4096);
    assert_eq!(plan.pk_filter_kind.as_deref(), Some("bloom"));
    assert_eq!(plan.pk_filter_columns, vec!["id"]);
    assert_eq!(
        plan.column_stats
            .get("_seq")
            .map(|stats| (&stats.min, &stats.max)),
        Some((&json!(1), &json!(10)))
    );
    assert!(plan.column_stats.contains_key("id"));
    assert!(plan.column_stats.contains_key("_commit_seq"));
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
