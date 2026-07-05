#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn export_contract_mentions_kalamdb_compatible_manifest_and_parquet() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let export = pg_koldstore::sql::ops::plan_koldstore_exec("EXPORT TABLE app.items").unwrap();

    assert_eq!(export.archive_manifest_path, "app/items/manifest.json");
    assert!(export.statement.sql.contains("koldstore.manifest"));
    assert!(export.statement.sql.contains("koldstore.cold_segments"));
}

#[tokio::test]
async fn export_query_reads_manifest_and_segments_from_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "export_compatibility").await?;
        let table = db.create_indexed_items_table("export_items", 12).await?;
        db.migrate_shared(&table.relation, "id").await?;
        assert_eq!(db.flush_table(&table.relation).await?, 12);

        let export = pg_koldstore::sql::ops::plan_koldstore_exec(&format!(
            "EXPORT TABLE {}",
            table.relation
        ))
        .unwrap();
        let executable_sql = export
            .statement
            .sql
            .replace("$1::regclass::oid", "$1::text::regclass::oid");
        let row = db
            .client
            .query_one(&executable_sql, &[&table.relation])
            .await?;

        let manifest_path = row.get::<_, String>(0);
        let object_path = row.get::<_, String>(1);
        let row_count = row.get::<_, i64>(2);
        let byte_size = row.get::<_, i64>(3);

        assert_eq!(
            export.archive_manifest_path,
            format!("{}/manifest.json", table.relation.replace('.', "/"))
        );
        assert!(manifest_path.ends_with("manifest.json"));
        assert!(object_path.ends_with(".parquet"));
        assert_eq!(row_count, 12);
        assert!(byte_size > 0);
    }

    Ok(())
}
