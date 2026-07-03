#[test]
fn sql_exposes_hydrate_pk_api() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    assert!(sql.contains("CREATE OR REPLACE FUNCTION koldstore.hydrate_pk"));
    assert!(sql.contains("lookup_cold"));
}
