#[test]
fn sql_extension_exposes_shared_greenfield_migration_contract() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    let spec = include_str!("../../../specs/001-pg-kalam-hot-cold-storage/spec.md");

    for needle in [
        "CREATE TYPE koldstore.managed_table_info",
        "CREATE TABLE IF NOT EXISTS koldstore.storage",
        "CREATE TABLE IF NOT EXISTS system.schemas",
        "CREATE TABLE IF NOT EXISTS koldstore.manifest",
        "CREATE SEQUENCE IF NOT EXISTS koldstore.global_seq",
        "PRIMARY KEY",
        "primary_key",
    ] {
        assert!(
            sql.contains(needle),
            "missing SQL contract fragment: {needle}"
        );
    }

    assert!(
        !sql.contains("USING koldstore"),
        "extension migration SQL must not implement a table access method"
    );
    assert!(spec.contains(
        "Greenfield workflow is normal `CREATE TABLE`, then `koldstore.migrate_table(...)`"
    ));
    assert_eq!(
        pg_koldstore::sql::session::snowflake_id() + 1,
        pg_koldstore::sql::session::snowflake_id()
    );
    assert_eq!(
        pg_koldstore::migrate::columns::REQUIRED_SYSTEM_COLUMNS,
        ["_seq", "_commit_seq", "_deleted"]
    );
}

#[test]
fn shared_greenfield_request_uses_no_scope_column() {
    let request = pg_koldstore::sql::ddl::MigrateTableRequest {
        table_name: "app.shared_items".to_string(),
        table_type: "shared".to_string(),
        storage_name: "local-minio".to_string(),
        flush_policy: Some("rows:1000,interval:60".to_string()),
        scope_column: None,
        options: serde_json::json!({}),
    };

    assert!(request.has_supported_table_type());
    assert!(request.has_valid_scope_arguments());
    assert_eq!(request.effective_scope_column(), None);
}
