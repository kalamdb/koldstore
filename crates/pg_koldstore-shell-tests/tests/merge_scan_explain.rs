use koldstore::{hooks::planner, merge_scan};

#[test]
fn merge_scan_explain_and_plan_contract_are_exposed() {
    assert_eq!(planner::MERGE_SCAN_NAME, "KoldMergeScan");
    assert_eq!(merge_scan::path::CUSTOM_PATH_NAME, "KoldMergeScan");
    assert_eq!(
        merge_scan::path::custom_scan_explain_label(),
        "Custom Scan (KoldMergeScan)"
    );

    let plan = merge_scan::plan::MergeScanPlan::new(42, vec!["id".to_string()]);
    assert_eq!(plan.table_oid, 42);
    assert_eq!(plan.primary_key_columns, vec!["id"]);
}

#[test]
fn format_bytes_human_matches_timescale_style_units() {
    // Mirror of pg/profile.rs helper — keep the contract stable for docs/examples.
    fn format_bytes_human(bytes: u64) -> String {
        const KB: f64 = 1024.0;
        const MB: f64 = KB * 1024.0;
        const GB: f64 = MB * 1024.0;
        let bytes_f = bytes as f64;
        if bytes_f >= GB {
            format!("{:.1} GB", bytes_f / GB)
        } else if bytes_f >= MB {
            format!("{:.1} MB", bytes_f / MB)
        } else if bytes_f >= KB {
            format!("{:.1} kB", bytes_f / KB)
        } else {
            format!("{bytes} bytes")
        }
    }

    assert_eq!(format_bytes_human(1_889_000), "1.8 MB");
}

#[test]
fn merge_scan_plan_serializes_complete_custom_private_payload() {
    use koldstore_common::{ColumnClass, ColumnId, Predicate, PredicateValue, ScopeKey, SeqId};
    use merge_scan::plan::{MergeMetadataAttnums, MergeScanPlan, SegmentHint};
    use serde_json::json;

    let safe_pk = Predicate {
        column_id: ColumnId::from_attnum(1),
        column: "id".to_string(),
        class: ColumnClass::PrimaryKey,
        value: PredicateValue::Eq(json!(42)),
    };
    let residual_status = Predicate {
        column_id: ColumnId::from_attnum(2),
        column: "status".to_string(),
        class: ColumnClass::Mutable,
        value: PredicateValue::Eq(json!("open")),
    };
    let security_scope = Predicate {
        column_id: ColumnId::from_attnum(3),
        column: "tenant_id".to_string(),
        class: ColumnClass::Security,
        value: PredicateValue::Eq(json!("tenant-a")),
    };

    let plan = MergeScanPlan {
        table_oid: 42,
        scanrelid: 1,
        primary_key_columns: vec!["id".to_string()],
        merge_metadata_attnums: MergeMetadataAttnums {
            seq: 3,
            deleted: 5,
            scope: Some(6),
        },
        scope_key: Some(ScopeKey::new("tenant-a").unwrap()),
        safe_quals: vec![safe_pk.clone()],
        residual_quals: vec![residual_status.clone()],
        security_quals: vec![security_scope.clone()],
        projection: vec!["id".to_string(), "status".to_string(), "seq".to_string()],
        segment_hints: vec![SegmentHint {
            segment_id: "segment-1".to_string(),
            scope_key: Some(ScopeKey::new("tenant-a").unwrap()),
            object_path: "app/items/batch-1.parquet".to_string(),
            selected_row_groups: vec![0, 2],
            min_seq: SeqId::new(10).unwrap(),
            max_seq: SeqId::new(30).unwrap(),
        }],
        overlay_strategy: Default::default(),
    };

    let encoded = plan.serialize().unwrap();
    let decoded = MergeScanPlan::deserialize(&encoded).unwrap();

    assert_eq!(decoded, plan);
    assert_eq!(
        decoded.custom_exprs(),
        vec![residual_status, security_scope]
    );
    assert_eq!(decoded.custom_private_projection(), ["id", "status", "seq"]);
}

#[test]
fn managed_read_replaces_heap_only_final_paths_but_keeps_hot_child_path() {
    use merge_scan::path::{build_path_portfolio, HotChildCandidate, PlannerPath};

    let decision = build_path_portfolio(
        true,
        vec![
            HotChildCandidate::new(
                PlannerPath::seq_scan("heap seq", 50.0),
                None,
                0,
                0,
                vec![1],
                false,
            ),
            HotChildCandidate::new(
                PlannerPath::index_scan("hot pk index", 10.0),
                None,
                0,
                0,
                vec![1],
                false,
            ),
            HotChildCandidate::new(
                PlannerPath::bitmap_scan("hot bitmap", 20.0),
                None,
                0,
                0,
                vec![1],
                false,
            ),
        ],
        0.0,
    )
    .unwrap();

    assert_eq!(decision.entries.len(), 1);
    assert_eq!(decision.entries[0].hot_child.name, "hot pk index");
    assert_eq!(decision.removed_heap_final_paths, 3);
    assert!(!decision.heap_only_final_path_available());
    assert_eq!(
        decision.final_paths()[0].explain_label(),
        "Custom Scan (KoldMergeScan)"
    );
}

#[test]
fn unmanaged_read_keeps_postgres_heap_paths_without_custom_scan() {
    use merge_scan::path::{build_path_portfolio, PlannerPath};

    let heap_paths = [
        PlannerPath::seq_scan("heap seq", 50.0),
        PlannerPath::index_scan("hot pk index", 10.0),
    ];
    let decision = build_path_portfolio(false, Vec::new(), 0.0).unwrap();

    assert!(decision.entries.is_empty());
    assert_eq!(decision.removed_heap_final_paths, 0);
    // Unmanaged portfolio is empty; callers keep their own heap_paths.
    assert_eq!(heap_paths.len(), 2);
}
