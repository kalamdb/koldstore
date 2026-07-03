#[test]
fn sql_extension_exposes_user_scoped_migration_contract() {
    let sql = include_str!("../sql/koldstore--0.1.0.sql");
    let spec = include_str!("../../../specs/001-pg-kalam-hot-cold-storage/spec.md");

    assert!(sql.contains("table_type text NOT NULL CHECK (table_type IN ('shared', 'user'))"));
    assert!(sql.contains("scope_column"));
    assert!(spec.contains("koldstore.user_id"));
    assert!(spec.contains("User-scoped tables MUST require a scope column"));
    assert!(pg_koldstore::migrate::columns::system_columns(true).contains(&"_user_id"));
}

#[test]
fn user_greenfield_request_defaults_to_system_scope_column() {
    let request = pg_koldstore::sql::ddl::MigrateTableRequest {
        table_name: "app.notes".to_string(),
        table_type: "user".to_string(),
        storage_name: "local-minio".to_string(),
        flush_policy: None,
        scope_column: None,
        options: serde_json::json!({}),
    };

    assert!(request.has_supported_table_type());
    assert!(request.has_valid_scope_arguments());
    assert_eq!(request.effective_scope_column(), Some("_user_id"));
}

#[test]
fn user_greenfield_request_preserves_explicit_scope_column() {
    let request = pg_koldstore::sql::ddl::MigrateTableRequest {
        table_name: "app.notes".to_string(),
        table_type: "user".to_string(),
        storage_name: "local-minio".to_string(),
        flush_policy: None,
        scope_column: Some("user_id".to_string()),
        options: serde_json::json!({}),
    };

    assert!(request.has_valid_scope_arguments());
    assert_eq!(request.effective_scope_column(), Some("user_id"));
}
