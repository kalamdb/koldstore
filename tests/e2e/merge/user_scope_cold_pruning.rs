#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use koldstore_common::{ScopeKey, SeqId};
use koldstore::merge_scan::exec::{begin_merge_scan_with_plan, ColdAvailability};
use koldstore::merge_scan::plan::{MergeScanPlan, SegmentHint};

#[test]
fn user_scope_cold_pruning_filters_segments_before_stream_open() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

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

#[tokio::test]
async fn user_scope_cold_segment_lookup_is_index_backed_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "user_scope_cold_pruning").await?;
        let table = db.create_user_notes_table("scope_notes").await?;
        db.manage_user_scoped(&table.relation, "user_id").await?;

        let plan = common::explain_with_seqscan_disabled(
            &db.client,
            &format!(
                r#"
                SELECT object_path
                FROM koldstore.cold_segments
                WHERE table_oid = '{}'::regclass::oid
                  AND scope_key = 'user-a'
                  AND status = 'active'
                  AND min_seq <= 5
                  AND max_seq >= 5
                ORDER BY min_seq, max_seq
                "#,
                table.relation
            ),
        )
        .await?;
        common::assertions::assert_catalog_index_plan_uses_any(
            &plan,
            &[
                "cold_segments_active_scope_seq_idx",
                "cold_segments_active_commit_idx",
            ],
        )?;
        assert!(plan.contains("scope_key = 'user-a'::text"), "{plan}");
        assert!(plan.contains("min_seq <= 5"), "{plan}");
        assert!(plan.contains("max_seq >= 5"), "{plan}");

        let rows = db
            .client
            .query(
                r#"
                SELECT object_path
                FROM koldstore.cold_segments
                WHERE table_oid = $1::text::regclass::oid
                  AND scope_key = 'user-a'
                  AND status = 'active'
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(rows.len(), 0);
    }

    Ok(())
}
