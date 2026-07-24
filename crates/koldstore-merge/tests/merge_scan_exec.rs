use koldstore_common::{ColdRow, CommitSeq, HotRow, LogicalPk, PkColumn, ScopeKey, SeqId};
use koldstore_merge::scan::exec::{
    begin_merge_scan, begin_merge_scan_with_plan, execute_merge_scan_with_filters,
    ColdAvailability, FilterPlan, ScanResourceCounters,
};
use koldstore_merge::scan::plan::{
    prune_segment_stats, retain_pre_merge_cold_prune_predicates, validate_prune_predicates_indexed,
    ColdPruneColumnPolicy, MergeMetadataAttnums, MergeScanPlan, SegmentHint, SegmentPrunePredicate,
    SegmentStatsHint,
};
use serde_json::json;
use std::collections::BTreeMap;

fn pk(id: i64) -> LogicalPk {
    LogicalPk::from_json_object(&json!({"id": id}), &[PkColumn::new("id").unwrap()]).unwrap()
}

fn hot(id: i64, seq: i64, commit_seq: i64, deleted: bool, status: &str) -> HotRow {
    HotRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted,
        row_image: json!({"id": id, "status": status}),
    }
}

fn cold(id: i64, seq: i64, commit_seq: i64, status: &str) -> ColdRow {
    ColdRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted: false,
        schema_version: 1,
        row_image: json!({"id": id, "status": status}),
    }
}

fn plan() -> MergeScanPlan {
    MergeScanPlan {
        table_oid: 42,
        scanrelid: 1,
        primary_key_columns: vec!["id".to_string()],
        merge_metadata_attnums: MergeMetadataAttnums {
            seq: 3,
            commit_seq: 4,
            deleted: 5,
            scope: None,
        },
        scope_key: None,
        safe_quals: Vec::new(),
        residual_quals: Vec::new(),
        security_quals: Vec::new(),
        projection: vec!["id".to_string(), "status".to_string()],
        segment_hints: vec![SegmentHint {
            segment_id: "segment-1".to_string(),
            scope_key: None,
            object_path: "app/items/batch-1.parquet".to_string(),
            selected_row_groups: vec![1],
            min_seq: SeqId::new(10).unwrap(),
            max_seq: SeqId::new(30).unwrap(),
        }],
        overlay_strategy: Default::default(),
    }
}

#[test]
fn scoped_segments_are_filtered_before_cold_streams_open() {
    let mut plan = plan();
    plan.scope_key = Some(ScopeKey::new("user-a").unwrap());
    plan.segment_hints = vec![
        SegmentHint {
            segment_id: "segment-user-a".to_string(),
            scope_key: Some(ScopeKey::new("user-a").unwrap()),
            object_path: "app/items/user-a/batch-1.parquet".to_string(),
            selected_row_groups: vec![0],
            min_seq: SeqId::new(1).unwrap(),
            max_seq: SeqId::new(10).unwrap(),
        },
        SegmentHint {
            segment_id: "segment-user-b".to_string(),
            scope_key: Some(ScopeKey::new("user-b").unwrap()),
            object_path: "app/items/user-b/batch-1.parquet".to_string(),
            selected_row_groups: vec![1],
            min_seq: SeqId::new(1).unwrap(),
            max_seq: SeqId::new(10).unwrap(),
        },
        SegmentHint {
            segment_id: "segment-shared".to_string(),
            scope_key: None,
            object_path: "app/items/shared/batch-1.parquet".to_string(),
            selected_row_groups: vec![2],
            min_seq: SeqId::new(1).unwrap(),
            max_seq: SeqId::new(10).unwrap(),
        },
    ];

    let state = begin_merge_scan_with_plan(&plan, ColdAvailability::Available).unwrap();

    assert_eq!(
        state.visible_segments,
        vec!["app/items/user-a/batch-1.parquet"]
    );
    assert_eq!(state.selected_row_groups, vec![0]);
    assert_eq!(state.resources.object_store_handles, 1);
}

#[test]
fn begin_merge_scan_loads_metadata_prunes_segments_and_opens_cold_streams() {
    let state = begin_merge_scan_with_plan(&plan(), ColdAvailability::Available).unwrap();

    assert_eq!(state.table_oid, 42);
    assert_eq!(state.visible_segments, vec!["app/items/batch-1.parquet"]);
    assert_eq!(state.selected_row_groups, vec![1]);
    assert!(state.snapshot_captured);
    assert!(state.cold_streams_open);
    assert_eq!(state.resources.object_store_handles, 1);
    assert_eq!(state.resources.arrow_buffers, 1);
}

#[test]
fn direct_begin_merge_scan_tracks_each_cold_segment_handle() {
    let state = begin_merge_scan(
        42,
        vec![
            "app/items/batch-1.parquet".to_string(),
            "app/items/batch-2.parquet".to_string(),
        ],
        ColdAvailability::Available,
    )
    .unwrap();

    assert_eq!(state.resources.object_store_handles, 2);
    assert_eq!(state.resources.arrow_buffers, 1);
}

#[test]
fn residual_and_security_quals_run_after_winner_resolution() {
    let result = execute_merge_scan_with_filters(
        vec![
            hot(1, 20, 20, false, "open"),
            hot(2, 21, 21, false, "closed"),
        ],
        vec![cold(1, 10, 10, "closed")],
        FilterPlan::new()
            .with_required_json_eq("status", "open")
            .with_security_json_eq("id", 1),
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].row_image["status"], "open");
    assert_eq!(result.filtered_rows, 1);
    assert_eq!(result.security_filtered_rows, 0);
}

#[test]
fn scan_state_cleanup_releases_resources_and_rescan_resets_merge_state() {
    let mut state = begin_merge_scan_with_plan(&plan(), ColdAvailability::Available).unwrap();
    state.resources = ScanResourceCounters {
        object_store_handles: 1,
        arrow_buffers: 2,
        memory_context_bytes: 4096,
    };

    state.cleanup();

    assert!(!state.cold_streams_open);
    assert!(state.visible_segments.is_empty());
    assert!(state.selected_row_groups.is_empty());
    assert_eq!(state.resources, ScanResourceCounters::default());

    state.rescan(&plan(), ColdAvailability::Available).unwrap();
    assert!(state.cold_streams_open);
    assert_eq!(state.visible_segments, vec!["app/items/batch-1.parquet"]);
}

#[test]
fn manifest_stats_pruning_skips_only_proven_non_overlapping_segments() {
    let segments = vec![
        SegmentStatsHint {
            object_path: "app/items/batch-1.parquet".to_string(),
            column_stats: BTreeMap::from([(
                "qty".to_string(),
                koldstore_parquet::ColumnStats {
                    min: json!(1),
                    max: json!(10),
                },
            )]),
            byte_size: None,
        },
        SegmentStatsHint {
            object_path: "app/items/batch-2.parquet".to_string(),
            column_stats: BTreeMap::from([(
                "qty".to_string(),
                koldstore_parquet::ColumnStats {
                    min: json!(50),
                    max: json!(99),
                },
            )]),
            byte_size: None,
        },
        SegmentStatsHint {
            object_path: "app/items/batch-missing-stats.parquet".to_string(),
            column_stats: BTreeMap::new(),
            byte_size: None,
        },
    ];

    let selected = prune_segment_stats(
        &segments,
        &[SegmentPrunePredicate::closed_range(
            "qty",
            json!(20),
            json!(60),
        )],
    );

    assert_eq!(
        selected,
        vec![
            "app/items/batch-2.parquet".to_string(),
            "app/items/batch-missing-stats.parquet".to_string(),
        ]
    );
}

#[test]
fn scope_equality_is_retained_for_pre_merge_cold_prune() {
    let predicates = vec![
        SegmentPrunePredicate::equality("id", json!(1)),
        SegmentPrunePredicate::equality("tenant_id", json!("tenant-a")),
        SegmentPrunePredicate::equality("conversation_id", json!("conv-1")),
        SegmentPrunePredicate::lower_bound("tenant_id", json!("tenant-m")),
    ];
    let retained = retain_pre_merge_cold_prune_predicates(predicates, |column| match column {
        "id" => Some(ColdPruneColumnPolicy {
            is_primary_key: true,
            is_scope: false,
            ordered_stats_safe: true,
            equality_stats_safe: true,
        }),
        "tenant_id" => Some(ColdPruneColumnPolicy {
            is_primary_key: false,
            is_scope: true,
            ordered_stats_safe: false,
            equality_stats_safe: true,
        }),
        "conversation_id" => Some(ColdPruneColumnPolicy {
            is_primary_key: false,
            is_scope: false,
            ordered_stats_safe: false,
            equality_stats_safe: true,
        }),
        _ => None,
    });

    assert_eq!(
        retained,
        vec![
            SegmentPrunePredicate::equality("id", json!(1)),
            SegmentPrunePredicate::equality("tenant_id", json!("tenant-a")),
        ]
    );
}

#[test]
fn text_scope_range_predicates_are_not_pre_merge_safe() {
    let retained = retain_pre_merge_cold_prune_predicates(
        vec![SegmentPrunePredicate::lower_bound(
            "tenant_id",
            json!("tenant-m"),
        )],
        |_| {
            Some(ColdPruneColumnPolicy {
                is_primary_key: false,
                is_scope: true,
                ordered_stats_safe: false,
                equality_stats_safe: true,
            })
        },
    );
    assert!(retained.is_empty());
}

#[test]
fn non_indexed_prune_predicates_are_rejected_before_cold_files_open() {
    let err = validate_prune_predicates_indexed(
        &[SegmentPrunePredicate::equality("status", json!("open"))],
        &["created_at".to_string()],
    )
    .unwrap_err();

    assert!(err.to_string().contains("status"));
    assert!(err.to_string().contains("indexed"));
}

#[test]
fn indexed_prune_predicates_keep_segments_without_manifest_stats() {
    let segments = vec![SegmentStatsHint {
        object_path: "app/items/batch-1.parquet".to_string(),
        column_stats: BTreeMap::new(),
        byte_size: None,
    }];
    let selected = prune_segment_stats(
        &segments,
        &[SegmentPrunePredicate::equality(
            "created_at",
            json!("2026-01-01"),
        )],
    );

    assert_eq!(selected, vec!["app/items/batch-1.parquet".to_string()]);
}
