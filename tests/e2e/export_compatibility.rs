#[test]
fn export_contract_mentions_kalamdb_compatible_manifest_and_parquet() {
    let sql = include_str!("../../sql/koldstore--0.1.0.sql");

    assert!(sql.contains("koldstore_exec('EXPORT TABLE ...')"));
    assert!(sql.contains("kalamdb-compatible manifest and Parquet archive"));
}
