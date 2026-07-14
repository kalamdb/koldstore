#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use koldstore::merge_scan::exec::{
    execute_merge_scan, execute_merge_scan_with_filters, FilterPlan,
};
use koldstore_common::{ColdRow, CommitSeq, HotRow, LogicalPk, PkColumn, SeqId};
use serde_json::json;

fn pk(id: i64) -> LogicalPk {
    let columns = vec![PkColumn::new("id").unwrap()];
    LogicalPk::from_json_object(&json!({"id": id}), &columns).unwrap()
}

fn hot(id: i64, seq: i64, commit_seq: i64, deleted: bool, body: &str) -> HotRow {
    HotRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted,
        row_image: json!({"id": id, "body": body}),
    }
}

fn cold(id: i64, seq: i64, commit_seq: i64, body: &str) -> ColdRow {
    ColdRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted: false,
        schema_version: 1,
        row_image: json!({"id": id, "body": body}),
    }
}

fn cold_deleted(id: i64, seq: i64, commit_seq: i64) -> ColdRow {
    ColdRow {
        pk: pk(id),
        scope_key: None,
        seq: SeqId::new(seq).unwrap(),
        commit_seq: CommitSeq::new(commit_seq).unwrap(),
        deleted: true,
        schema_version: 1,
        row_image: json!({"id": id}),
    }
}

#[test]
fn merge_scan_results_resolve_hot_winner_and_tombstone_masking() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let result = execute_merge_scan(
        vec![
            hot(1, 20, 20, false, "hot-winner"),
            hot(2, 21, 21, true, "deleted"),
        ],
        vec![
            cold(1, 10, 10, "older-cold"),
            cold(2, 10, 10, "masked-cold"),
        ],
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].row_image,
        json!({"id": 1, "body": "hot-winner"})
    );
    assert_eq!(result.hot_rows_seen, 2);
    assert_eq!(result.cold_rows_seen, 2);
    assert_eq!(result.tombstones_masked, 1);
}

#[test]
fn merge_scan_results_apply_residual_filters_after_winner_resolution() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let result = execute_merge_scan_with_filters(
        vec![hot(1, 20, 20, false, "hot-winner")],
        vec![cold(1, 10, 10, "older-cold"), cold(2, 11, 11, "cold-only")],
        FilterPlan::new().with_required_json_eq("body", "hot-winner"),
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.filtered_rows, 1);
    assert_eq!(
        result.rows[0].row_image,
        json!({"id": 1, "body": "hot-winner"})
    );
}

#[test]
fn merge_scan_results_apply_cold_delete_markers_and_newer_reinserts() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let deleted = execute_merge_scan(
        vec![],
        vec![cold(1, 10, 10, "old"), cold_deleted(1, 20, 20)],
    )
    .unwrap();
    assert!(deleted.rows.is_empty());
    assert_eq!(deleted.tombstones_masked, 0);

    let reinserted = execute_merge_scan(
        vec![hot(1, 30, 30, false, "reinserted")],
        vec![cold(1, 10, 10, "old"), cold_deleted(1, 20, 20)],
    )
    .unwrap();
    assert_eq!(reinserted.rows.len(), 1);
    assert_eq!(
        reinserted.rows[0].row_image,
        json!({"id": 1, "body": "reinserted"})
    );
}

#[tokio::test]
async fn flushed_table_prunes_hot_rows_and_keeps_cold_payload_for_merge_reads() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_scan_results").await?;
        let table = db
            .create_indexed_items_table("merge_result_items", 16)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 16).await?;

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {} (id, account_id, title, qty, category)
                VALUES (100, 1, 'new-hot', 100, 'new');
                "#,
                table.relation
            ))
            .await?;

        let cold_count = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get::<_, i64>(0);
        assert_eq!(
            cold_count, 17,
            "managed SELECT must merge 16 cold rows plus 1 hot row"
        );

        let rows = db
            .client
            .query(
                &format!(
                    "SELECT id, title FROM {} WHERE id IN (1, 2, 100) ORDER BY id",
                    table.relation
                ),
                &[],
            )
            .await?;
        let visible = rows
            .into_iter()
            .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();

        assert_eq!(
            visible,
            vec![
                (1, "item-000001".to_string()),
                (2, "item-000002".to_string()),
                (100, "new-hot".to_string()),
            ]
        );

        let planned = common::explain(
            &db.client,
            &format!("SELECT count(*) FROM {}", table.relation),
        )
        .await?;
        common::assert_kold_merge_scan_cold_reads(&planned, "manifest.json", 1)?;

        let analyzed = common::explain_analyze(
            &db.client,
            &format!("SELECT count(*) FROM {}", table.relation),
        )
        .await?;
        common::assert_kold_merge_scan_executed_cold_reads(&analyzed, 1)?;

        common::assert_cold_metadata_present(&db.client, &table.relation).await?;

        let status = common::describe_table(&db.client, &table.relation).await?;
        assert_eq!(status.hot_rows, 1);
        assert_eq!(status.mirror_rows, 1);
        assert!(status.cold_row_count >= 16);
    }

    Ok(())
}

#[tokio::test]
async fn merge_scan_preserves_native_hot_plan_and_masks_preflush_deletes() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_scan_overlay").await?;
        let table = db
            .create_indexed_items_table("merge_overlay_items", 8)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;

        // Point lookup should keep a native hot child under KoldMergeScan.
        // After flush the hot heap is empty, so Seq Scan can beat Index Scan;
        // insert one hot row and re-check that an index child is selectable.
        let planned = common::explain(
            &db.client,
            &format!("SELECT title FROM {} WHERE id = 1", table.relation),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&planned)?;
        anyhow::ensure!(
            planned.contains("Hot Plan:"),
            "expected Hot Plan in EXPLAIN, got:\n{planned}"
        );
        anyhow::ensure!(
            planned.contains("Seq Scan")
                || planned.contains("Index Scan")
                || planned.contains("Bitmap Heap Scan"),
            "expected native hot child plan in EXPLAIN, got:\n{planned}"
        );

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {} (id, account_id, title, qty, category)
                VALUES (1000, 1, 'hot-index', 1, 'hot');
                SET enable_seqscan = off;
                "#,
                table.relation
            ))
            .await?;
        let planned_hot = common::explain(
            &db.client,
            &format!("SELECT title FROM {} WHERE id = 1000", table.relation),
        )
        .await?;
        db.client.batch_execute("SET enable_seqscan = on").await?;
        anyhow::ensure!(
            planned_hot.contains("Hot Plan:") && planned_hot.contains("Index Scan"),
            "expected Hot Plan: Index Scan for hot PK lookup, got:\n{planned_hot}"
        );

        // Committed delete of a previously cold PK must be invisible before flush.
        // Rematerialize into hot first so DELETE writes a mirror tombstone (op=3);
        // a cold-only DELETE that matches zero heap rows does not fire triggers.
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, account_id, title, qty, category)
                VALUES (2, 1, 'rematerialized', 2, 'hot')
                ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDED.title, qty = EXCLUDED.qty, category = EXCLUDED.category;
                DELETE FROM {relation} WHERE id = 2;
                "#,
                relation = table.relation
            ))
            .await?;
        let count: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*) FROM {} WHERE id = 2", table.relation),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            count == 0,
            "cold row must stay invisible after committed delete before flush, count={count}"
        );

        let analyzed = common::explain_analyze(
            &db.client,
            &format!("SELECT count(*) FROM {}", table.relation),
        )
        .await?;
        anyhow::ensure!(
            analyzed.contains("Mirror Tombstones"),
            "expected Mirror Tombstones in EXPLAIN ANALYZE, got:\n{analyzed}"
        );

        // enable_merge_scan=off must error on managed SELECT, not heap-only.
        db.client
            .batch_execute("SET koldstore.enable_merge_scan = off")
            .await?;
        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await;
        anyhow::ensure!(
            err.is_err(),
            "enable_merge_scan=off must error on managed SELECT"
        );
        db.client
            .batch_execute("SET koldstore.enable_merge_scan = on")
            .await?;
    }

    Ok(())
}
