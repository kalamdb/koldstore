#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use koldstore_core::{ColdRow, CommitSeq, HotRow, LogicalPk, PkColumn, SeqId};
use pg_koldstore::merge_scan::exec::{
    execute_merge_scan, execute_merge_scan_with_filters, FilterPlan,
};
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

#[tokio::test]
async fn flushed_table_and_later_hot_dml_return_current_rows_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_scan_results").await?;
        let table = db
            .create_indexed_items_table("merge_result_items", 16)
            .await?;
        db.migrate_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;

        db.client
            .batch_execute(&format!(
                r#"
                UPDATE {relation}
                SET title = 'hot-winner'
                WHERE id = 1;

                DELETE FROM {relation}
                WHERE id = 2;

                INSERT INTO {relation} (id, account_id, title, qty, category)
                VALUES (100, 1, 'new-hot', 100, 'new');
                "#,
                relation = table.relation
            ))
            .await?;

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
            vec![(1, "hot-winner".to_string()), (100, "new-hot".to_string())]
        );
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;
    }

    Ok(())
}
