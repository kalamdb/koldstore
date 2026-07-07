#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn merge_scan_matrix_targets_active_pgrx_versions() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    assert_eq!(
        common::local_pg_matrix()
            .into_iter()
            .map(|target| target.version)
            .collect::<Vec<_>>(),
        common::expected_pg_versions()
    );
}

#[test]
fn merge_scan_matrix_covers_results_explain_residual_quals_and_outage() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let required_assertions = [
        "merged_select",
        "custom_scan_explain",
        "residual_quals_after_winner",
        "cold_outage_error",
    ];

    assert!(required_assertions.contains(&"merged_select"));
    assert!(required_assertions.contains(&"custom_scan_explain"));
    assert!(required_assertions.contains(&"residual_quals_after_winner"));
    assert!(required_assertions.contains(&"cold_outage_error"));
}

#[tokio::test]
async fn managed_select_uses_merge_scan_after_flush_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_scan_matrix").await?;
        let table = db.create_indexed_items_table("merge_items", 1_000).await?;
        db.migrate_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 1_000).await?;

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, account_id, title, qty, category)
                VALUES (1001, 1, 'hot-after-flush-only', 1, 'hot-after-flush')
                ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDED.title;
                ANALYZE {relation};
                "#,
                relation = table.relation
            ))
            .await?;

        let plan = common::explain(
            &db.client,
            &format!(
                "SELECT id, title FROM {} WHERE title = 'hot-after-flush-only'",
                table.relation
            ),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan)?;
        common::assert_kold_merge_scan_cold_reads(&plan, "manifest.json", 1)?;
        assert!(
            plan.contains("Filter:") && plan.contains("hot-after-flush-only"),
            "expected filtered merge scan plan, got:\n{plan}"
        );

        let row = db
            .client
            .query_one(
                &format!(
                    "SELECT id, title FROM {} WHERE title = 'hot-after-flush-only'",
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(row.get::<_, i64>(0), 1001);
        assert_eq!(row.get::<_, String>(1), "hot-after-flush-only");
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;
    }

    Ok(())
}
