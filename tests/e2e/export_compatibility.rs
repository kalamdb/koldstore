#[test]
fn export_contract_mentions_kalamdb_compatible_manifest_and_parquet() {
    let export = pg_koldstore::sql::ops::plan_koldstore_exec("EXPORT TABLE app.items").unwrap();

    assert_eq!(export.archive_manifest_path, "app/items/manifest.json");
    assert!(export.statement.sql.contains("koldstore.manifest"));
    assert!(export.statement.sql.contains("koldstore.cold_segments"));
}
