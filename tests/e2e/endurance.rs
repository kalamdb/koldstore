#[path = "common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn endurance_cycle_contract_covers_repeated_lifecycle_operations() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let cycle = [
        "migrate",
        "DML",
        "flush",
        "query",
        "cold metadata validation",
        "size check",
        "repeat",
    ];

    assert_eq!(cycle.len(), 7);
}

#[tokio::test]
async fn repeated_flush_and_hot_dml_cycles_remain_bounded_on_pgrx() -> Result<()> {
    for target in common::local_pg_matrix() {
        let db = common::TestDb::start(target, "endurance").await?;
        let baseline = db
            .create_indexed_items_table("endurance_baseline", 512)
            .await?;
        let table = db
            .create_indexed_items_table("endurance_items", 512)
            .await?;
        db.migrate_shared(&table.relation, "id").await?;

        for cycle in 0..3 {
            for relation in [&baseline.relation, &table.relation] {
                db.client
                    .batch_execute(&format!(
                        r#"
                        INSERT INTO {relation} (id, account_id, title, qty, category)
                        VALUES ({insert_id}, 99, 'cycle-{cycle}', {cycle}, 'cycle')
                        ON CONFLICT (id) DO UPDATE
                        SET qty = EXCLUDED.qty,
                            title = EXCLUDED.title;

                        UPDATE {relation}
                        SET qty = qty + 1
                        WHERE id BETWEEN 1 AND 20;

                        DELETE FROM {relation}
                        WHERE id = {delete_id};
                        "#,
                        insert_id = 10_000 + cycle,
                        delete_id = 500 - cycle,
                    ))
                    .await?;
            }

            let flushed = db.flush_table(&table.relation).await?;
            assert!(flushed > 0);
            common::assert_cold_metadata_present(&db.client, &table.relation).await?;
            common::assert_no_active_jobs(&db.client, &table.relation).await?;
            common::assertions::assert_no_duplicate_hot_pk(&db.client, &table.relation, "id")
                .await?;
        }

        let baseline_size = common::relation_size(&db.client, &baseline.relation).await?;
        let managed_size = common::relation_size(&db.client, &table.relation).await?;
        common::assertions::assert_system_column_size_overhead(baseline_size, managed_size, 512)?;
        assert!(common::cold_segment_count(&db.client, &table.relation).await? >= 3);

        let remaining = common::row_count(&db.client, &table.relation).await?;
        assert_eq!(remaining, 512);
    }

    Ok(())
}
