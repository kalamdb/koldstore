#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn demigrate_matrix_targets_active_pgrx_versions() {
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
fn demigrate_matrix_covers_flush_cold_delete_and_user_scoped_tables() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let scenarios = [
        "demigrate_after_flush",
        "demigrate_after_cold_only_delete",
        "demigrate_user_scoped_table",
    ];

    assert!(scenarios.contains(&"demigrate_after_flush"));
    assert!(scenarios.contains(&"demigrate_after_cold_only_delete"));
    assert!(scenarios.contains(&"demigrate_user_scoped_table"));
}

#[tokio::test]
async fn demigrate_catalog_deactivation_cancels_jobs_and_preserves_heap_rows_on_pgrx() -> Result<()>
{
    for target in common::local_pg_matrix() {
        let db = common::TestDb::start(target, "demigrate_matrix").await?;
        let table = db.create_indexed_items_table("demigrate_items", 40).await?;
        db.migrate_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;
        db.insert_pending_flush_job(&table.relation, "").await?;
        assert_eq!(
            common::active_job_count(&db.client, &table.relation).await?,
            1
        );

        let deactivated = db
            .client
            .query_one(
                "SELECT koldstore.demigrate_table($1::text::regclass, false, false, true)",
                &[&table.relation],
            )
            .await?;
        assert_eq!(deactivated.get::<_, i64>(0), 1);

        assert_eq!(
            common::active_job_count(&db.client, &table.relation).await?,
            0
        );
        let active_schema_rows = db
            .client
            .query_one(
                "SELECT count(*) FROM koldstore.schemas WHERE table_oid = $1::text::regclass::oid AND active",
                &[&table.relation],
            )
            .await?
            .get::<_, i64>(0);
        assert_eq!(active_schema_rows, 0);
        assert_eq!(common::row_count(&db.client, &table.relation).await?, 40);

        let system_columns = db
            .client
            .query_one(
                "SELECT count(*) FROM pg_attribute WHERE attrelid = $1::text::regclass AND attname = ANY($2) AND NOT attisdropped",
                &[&table.relation, &&["_seq", "_commit_seq", "_deleted"][..]],
            )
            .await?
            .get::<_, i64>(0);
        assert_eq!(system_columns, 0);
    }

    Ok(())
}
