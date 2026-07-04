use std::collections::BTreeMap;

use koldstore_core::{CommitSeq, SeqId};
use koldstore_parquet::{
    ColumnStats, FooterSummary, ParquetReadOptions, RowGroupPruner, RowGroupStats,
};
use serde_json::json;

#[test]
fn reader_options_capture_projection_seq_range_and_pk_values() {
    let options = ParquetReadOptions::new()
        .with_columns(["id", "_seq"])
        .with_row_groups([1, 3])
        .with_seq_range("_seq", SeqId::new(10).unwrap(), SeqId::new(20).unwrap())
        .with_commit_seq_range(
            "_commit_seq",
            CommitSeq::new(11).unwrap(),
            CommitSeq::new(21).unwrap(),
        )
        .with_pk_values("id", ["42"]);

    assert_eq!(options.columns, vec!["id", "_seq"]);
    assert_eq!(options.row_groups.as_ref().unwrap(), &vec![1, 3]);
    assert_eq!(options.seq_range.as_ref().unwrap().min.get(), 10);
    assert_eq!(options.commit_seq_range.as_ref().unwrap().max.get(), 21);
    assert_eq!(options.pk_values.as_ref().unwrap().values, vec!["42"]);
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
