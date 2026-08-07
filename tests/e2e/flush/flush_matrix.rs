use crate::common;

use anyhow::Result;

#[tokio::test]
async fn flush_matrix_covers_small_and_larger_batches_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_matrix").await?;

        for (table_name, rows) in [("flush_matrix_small", 1_i64), ("flush_matrix_large", 128)] {
            let table = db.create_indexed_items_table(table_name, rows).await?;
            db.manage_shared(&table.relation, "id").await?;
            let flushed = db.flush_table(&table.relation).await?;
            assert_eq!(flushed, rows);
            common::assert_cold_metadata_present(&db.client, &table.relation).await?;
            common::assert_flush_pruned_hot_storage(&db.client, &table.relation, rows).await?;
            common::assert_no_active_jobs(&db.client, &table.relation).await?;

            let plan = common::explain(
                &db.client,
                &format!(
                    "SELECT id FROM {} WHERE title = 'item-000001'",
                    table.relation
                ),
            )
            .await?;
            common::assert_kold_merge_scan_explain(&plan)?;
            common::assert_kold_merge_scan_cold_reads(&plan, "manifest.json", 1)?;
            assert!(
                plan.contains("Filter:") && plan.contains("item-000001"),
                "expected filtered merge scan plan, got:\n{plan}"
            );
        }
    }

    Ok(())
}
