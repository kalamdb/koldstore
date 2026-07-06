use koldstore_common::{ColdRow, CommitSeq, HotRow, LogicalPk, PkColumn, ScopeKey, SeqId};
use pg_koldstore::merge_scan::exec::{
    begin_merge_scan, begin_merge_scan_with_plan, execute_merge_scan_with_filters,
    ColdAvailability, FilterPlan, ScanResourceCounters,
};
use pg_koldstore::merge_scan::plan::{MergeMetadataAttnums, MergeScanPlan, SegmentHint};
use serde_json::json;

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
