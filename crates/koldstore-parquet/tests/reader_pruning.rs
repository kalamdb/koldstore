use koldstore_parquet::{FooterSummary, ParquetReadOptions, RowGroupPruner, RowGroupStats};

#[test]
fn reader_options_capture_projection_seq_range_and_pk_values() {
    let options = ParquetReadOptions::new()
        .with_columns(["id", "_seq"])
        .with_seq_range("_seq", 10, 20)
        .with_pk_values("id", ["42"]);

    assert_eq!(options.columns, vec!["id", "_seq"]);
    assert_eq!(options.seq_range.as_ref().unwrap().min, 10);
    assert_eq!(options.pk_values.as_ref().unwrap().values, vec!["42"]);
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

    let decision = RowGroupPruner.prune_seq_range(&footer, 10, 20);

    assert_eq!(decision.selected_row_groups, vec![1]);
    assert_eq!(decision.skipped_row_groups, 1);
}
