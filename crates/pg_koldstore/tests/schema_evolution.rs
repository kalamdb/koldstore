#[test]
fn schema_evolution_contract_covers_add_column_and_type_rejection() {
    let spec = include_str!("../../../specs/001-pg-kalam-hot-cold-storage/spec.md");
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    assert!(spec.contains("schema"));
    assert!(sql.contains("schema_version"));
    assert!(!pg_koldstore::migrate::constraints::type_supported("bytea"));
    assert!(pg_koldstore::migrate::constraints::type_supported("jsonb"));
}
