use koldstore_common::SeqId;
use koldstore_merge::scan::plan::{
    group_segments_newest_first, group_segments_oldest_first,
    retain_pre_merge_cold_prune_predicates, validate_prune_predicates_indexed,
    ColdPruneColumnPolicy, SegmentPrunePredicate, SegmentStatsHint,
};
use koldstore_sortkey::SortKeyValue;
#[test]
fn primary_key_predicates_are_retained_for_pre_merge_cold_prune() {
    let predicates = vec![
        SegmentPrunePredicate::equality(1, "id", SortKeyValue::Int8(1)),
        SegmentPrunePredicate::equality(2, "tenant_id", SortKeyValue::Int8(1)),
        SegmentPrunePredicate::equality(3, "conversation_id", SortKeyValue::Int8(1)),
        SegmentPrunePredicate::lower_bound(2, "tenant_id", SortKeyValue::Int8(2)),
    ];
    let retained = retain_pre_merge_cold_prune_predicates(predicates, |column| match column {
        1 => Some(ColdPruneColumnPolicy {
            is_primary_key: true,
            is_scope: false,
            is_order_column: false,
            sort_key_indexable: true,
        }),
        2 => Some(ColdPruneColumnPolicy {
            is_primary_key: false,
            is_scope: true,
            is_order_column: false,
            // Text scope is not Sort Key V1–indexable; residual only.
            sort_key_indexable: false,
        }),
        3 => Some(ColdPruneColumnPolicy {
            is_primary_key: false,
            is_scope: false,
            is_order_column: false,
            sort_key_indexable: false,
        }),
        _ => None,
    });

    assert_eq!(
        retained,
        vec![SegmentPrunePredicate::equality(
            1,
            "id",
            SortKeyValue::Int8(1)
        )]
    );
}

#[test]
fn text_scope_predicates_are_not_pre_merge_safe_without_sort_key() {
    let retained = retain_pre_merge_cold_prune_predicates(
        vec![
            SegmentPrunePredicate::equality(2, "tenant_id", SortKeyValue::Int8(1)),
            SegmentPrunePredicate::lower_bound(2, "tenant_id", SortKeyValue::Int8(2)),
        ],
        |_| {
            Some(ColdPruneColumnPolicy {
                is_primary_key: false,
                is_scope: true,
                is_order_column: false,
                sort_key_indexable: false,
            })
        },
    );
    assert!(retained.is_empty());
}

#[test]
fn sort_key_scope_and_order_column_predicates_are_pre_merge_safe() {
    let retained = retain_pre_merge_cold_prune_predicates(
        vec![
            SegmentPrunePredicate::equality(2, "tenant_id", SortKeyValue::Int8(7)),
            SegmentPrunePredicate::lower_bound(4, "event_time", SortKeyValue::Int8(100)),
            SegmentPrunePredicate::equality(3, "payload", SortKeyValue::Int8(1)),
        ],
        |column| match column {
            2 => Some(ColdPruneColumnPolicy {
                is_primary_key: false,
                is_scope: true,
                is_order_column: false,
                sort_key_indexable: true,
            }),
            4 => Some(ColdPruneColumnPolicy {
                is_primary_key: false,
                is_scope: false,
                is_order_column: true,
                sort_key_indexable: true,
            }),
            _ => Some(ColdPruneColumnPolicy {
                is_primary_key: false,
                is_scope: false,
                is_order_column: false,
                sort_key_indexable: true,
            }),
        },
    );
    assert_eq!(
        retained,
        vec![
            SegmentPrunePredicate::equality(2, "tenant_id", SortKeyValue::Int8(7)),
            SegmentPrunePredicate::lower_bound(4, "event_time", SortKeyValue::Int8(100)),
        ]
    );
}

#[test]
fn non_indexed_prune_predicates_are_rejected_before_cold_files_open() {
    let err = validate_prune_predicates_indexed(
        &[SegmentPrunePredicate::equality(
            2,
            "status",
            SortKeyValue::Int8(1),
        )],
        &[3],
    )
    .unwrap_err();

    assert!(err.to_string().contains("status"));
    assert!(err.to_string().contains("indexed"));
}

fn versioned_segment(path: &str, min_seq: i64, max_seq: i64) -> SegmentStatsHint {
    SegmentStatsHint {
        object_path: path.to_string(),
        schema_version: 1,
        physical_names: Default::default(),
        byte_size: None,
        min_seq: SeqId::new(min_seq).unwrap(),
        max_seq: SeqId::new(max_seq).unwrap(),
        selected_row_groups: None,
    }
}

#[test]
fn newest_first_segment_groups_keep_disjoint_payloads_separate() {
    let groups = group_segments_newest_first(vec![
        versioned_segment("old.parquet", 1, 10),
        versioned_segment("new.parquet", 21, 30),
        versioned_segment("middle.parquet", 11, 20),
    ])
    .unwrap();

    let paths = groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|segment| segment.object_path.as_str())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        vec![
            vec!["new.parquet"],
            vec!["middle.parquet"],
            vec!["old.parquet"]
        ]
    );
}

#[test]
fn newest_first_segment_groups_combine_transitive_overlaps() {
    let groups = group_segments_newest_first(vec![
        versioned_segment("old.parquet", 1, 9),
        versioned_segment("bridge.parquet", 18, 25),
        versioned_segment("new.parquet", 20, 30),
        versioned_segment("middle.parquet", 10, 20),
    ])
    .unwrap();

    assert_eq!(groups.len(), 2);
    assert_eq!(
        groups[0]
            .iter()
            .map(|segment| segment.object_path.as_str())
            .collect::<Vec<_>>(),
        vec!["new.parquet", "bridge.parquet", "middle.parquet"]
    );
    assert_eq!(groups[1][0].object_path, "old.parquet");
}

#[test]
fn newest_first_segment_groups_reject_reversed_sequence_range() {
    let error = group_segments_newest_first(vec![SegmentStatsHint {
        object_path: "reversed.parquet".to_string(),
        schema_version: 1,
        physical_names: Default::default(),
        byte_size: None,
        min_seq: SeqId::new(20).unwrap(),
        max_seq: SeqId::new(10).unwrap(),
        selected_row_groups: None,
    }])
    .unwrap_err();

    assert!(error.to_string().contains("reversed.parquet"));
    assert!(error.to_string().contains("seq"));
}

#[test]
fn oldest_first_segment_groups_stream_disjoint_ranges_forward() {
    let groups = group_segments_oldest_first(vec![
        versioned_segment("new.parquet", 21, 30),
        versioned_segment("old.parquet", 1, 10),
        versioned_segment("middle.parquet", 11, 20),
    ])
    .unwrap();

    let paths = groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|segment| segment.object_path.as_str())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        vec![
            vec!["old.parquet"],
            vec!["middle.parquet"],
            vec!["new.parquet"]
        ]
    );
}

#[test]
fn oldest_first_segment_groups_keep_transitive_overlaps_atomic() {
    let groups = group_segments_oldest_first(vec![
        versioned_segment("old.parquet", 1, 9),
        versioned_segment("bridge.parquet", 18, 25),
        versioned_segment("new.parquet", 20, 30),
        versioned_segment("middle.parquet", 10, 20),
    ])
    .unwrap();

    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0][0].object_path, "old.parquet");
    assert_eq!(
        groups[1]
            .iter()
            .map(|segment| segment.object_path.as_str())
            .collect::<Vec<_>>(),
        vec!["middle.parquet", "bridge.parquet", "new.parquet"]
    );
}
