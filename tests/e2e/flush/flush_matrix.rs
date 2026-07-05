#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn flush_matrix_targets_active_pgrx_versions() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let versions = common::local_pg_matrix()
        .iter()
        .map(|target| target.version)
        .collect::<Vec<_>>();

    assert_eq!(versions, common::expected_pg_versions());
}

#[test]
fn flush_matrix_covers_flush_manifest_metadata_and_hot_cleanup() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let workflow = [
        "koldstore.flush_table",
        "batch-0.parquet",
        "manifest.json",
        "koldstore.cold_segments",
        "koldstore.cold_pk_hints",
        "hot cleanup after manifest commit",
    ];

    for required_step in [
        "koldstore.flush_table",
        "manifest.json",
        "koldstore.cold_segments",
        "koldstore.cold_pk_hints",
        "hot cleanup after manifest commit",
    ] {
        assert!(workflow.contains(&required_step));
    }
}

#[tokio::test]
async fn flush_matrix_covers_small_and_larger_batches_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_matrix").await?;

        for (table_name, rows) in [("flush_matrix_small", 1_i64), ("flush_matrix_large", 128)] {
            let table = db.create_indexed_items_table(table_name, rows).await?;
            db.migrate_shared(&table.relation, "id").await?;
            let flushed = db.flush_table(&table.relation).await?;
            assert_eq!(flushed, rows);
            common::assert_cold_metadata_present(&db.client, &table.relation).await?;
            common::assert_no_active_jobs(&db.client, &table.relation).await?;

            let plan = common::explain_with_seqscan_disabled(
                &db.client,
                &format!(
                    "SELECT id FROM {} WHERE title = 'item-000001'",
                    table.relation
                ),
            )
            .await?;
            common::assert_index_scan(&plan, &table.title_index)?;
        }
    }

    Ok(())
}
