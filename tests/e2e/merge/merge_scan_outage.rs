use crate::common;

use anyhow::Result;

#[tokio::test]
async fn dirty_manifest_outage_state_uses_partial_index_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_scan_outage").await?;
        let table = db.create_indexed_items_table("outage_items", 8).await?;
        db.manage_shared(&table.relation, "id").await?;

        let plan = common::explain_with_seqscan_disabled(
            &db.client,
            r#"
            SELECT table_oid, scope_key
            FROM koldstore.manifest
            WHERE sync_state IN ('pending_write', 'stale', 'error')
            ORDER BY updated_at
            "#,
        )
        .await?;
        common::assertions::assert_catalog_index_plan(&plan, "manifest_dirty_idx")?;
    }

    Ok(())
}
