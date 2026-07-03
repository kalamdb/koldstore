use koldstore_core::{ScopeKey, SeqId};
use pg_koldstore::merge_scan::exec::{begin_merge_scan_with_plan, ColdAvailability};
use pg_koldstore::merge_scan::plan::{MergeScanPlan, SegmentHint};

#[test]
fn user_scope_cold_pruning_filters_segments_before_stream_open() {
    let mut plan = MergeScanPlan::new(42, vec!["id".to_string()]);
    plan.scope_key = Some(ScopeKey::new("user-a").unwrap());
    plan.segment_hints = vec![
        SegmentHint {
            segment_id: "segment-a".to_string(),
            scope_key: Some(ScopeKey::new("user-a").unwrap()),
            object_path: "app/notes/user-a/batch-1.parquet".to_string(),
            selected_row_groups: vec![0],
            min_seq: SeqId::new(1).unwrap(),
            max_seq: SeqId::new(2).unwrap(),
        },
        SegmentHint {
            segment_id: "segment-b".to_string(),
            scope_key: Some(ScopeKey::new("user-b").unwrap()),
            object_path: "app/notes/user-b/batch-1.parquet".to_string(),
            selected_row_groups: vec![1],
            min_seq: SeqId::new(1).unwrap(),
            max_seq: SeqId::new(2).unwrap(),
        },
    ];

    let state = begin_merge_scan_with_plan(&plan, ColdAvailability::Available).unwrap();

    assert_eq!(
        state.visible_segments,
        vec!["app/notes/user-a/batch-1.parquet"]
    );
    assert_eq!(state.selected_row_groups, vec![0]);
    assert_eq!(state.resources.object_store_handles, 1);
}
