use crate::common;

use anyhow::Result;

#[test]
fn quickstart_matrix_covers_all_documented_scenarios() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let quickstart = include_str!("../../../specs/001-pg-kalam-hot-cold-storage/quickstart.md");
    let scenario_count = quickstart.matches("## Scenario ").count();

    assert!(scenario_count >= 10);
    assert_eq!(
        common::local_pg_matrix()
            .into_iter()
            .map(|target| target.version)
            .collect::<Vec<_>>(),
        common::expected_pg_versions()
    );
}

#[tokio::test]
async fn quickstart_managed_table_keeps_size_and_index_overhead_bounded() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "quickstart_matrix").await?;
        let baseline = db
            .create_indexed_items_table("quickstart_baseline", 2_000)
            .await?;
        let managed = db
            .create_indexed_items_table("quickstart_managed", 2_000)
            .await?;

        let baseline_size = common::relation_size(&db.client, &baseline.relation).await?;
        db.manage_shared(&managed.relation, "id").await?;
        let managed_size = common::relation_size(&db.client, &managed.relation).await?;

        common::assertions::assert_system_column_size_overhead(baseline_size, managed_size, 2_000)?;
        common::assertions::assert_no_duplicate_hot_pk(&db.client, &managed.relation, "id").await?;

        let plan = common::explain(
            &db.client,
            &format!(
                "SELECT id, title FROM {} WHERE title = 'item-000777'",
                managed.relation
            ),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan)?;
        assert!(
            plan.contains("Filter:") && plan.contains("item-000777"),
            "expected filtered merge scan plan, got:\n{plan}"
        );
    }

    Ok(())
}
