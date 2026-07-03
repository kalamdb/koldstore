#[test]
fn migration_sql_rejects_tables_without_primary_key() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    assert!(sql.contains("managed tables require a PRIMARY KEY"));
    assert!(sql.contains("idx.indisprimary"));
}
