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
async fn managed_hot_read_preserves_indexed_access_path_on_pgrx() -> Result<()> {
    for target in common::local_pg_matrix() {
        let db = common::TestDb::start(target, "merge_scan_matrix").await?;
        let table = db.create_indexed_items_table("merge_items", 1_000).await?;
        db.migrate_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;

        let plan = common::explain_with_seqscan_disabled(
            &db.client,
            &format!(
                "SELECT id, title FROM {} WHERE title = 'item-000333'",
                table.relation
            ),
        )
        .await?;
        common::assert_index_scan(&plan, &table.title_index)?;

        let row = db
            .client
            .query_one(
                &format!(
                    "SELECT id, title FROM {} WHERE title = 'item-000333'",
                    table.relation
                ),
                &[],
            )
            .await?;
        assert_eq!(row.get::<_, i64>(0), 333);
        assert_eq!(row.get::<_, String>(1), "item-000333");
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;
    }

    Ok(())
}
