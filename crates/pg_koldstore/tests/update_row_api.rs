#[test]
fn sql_exposes_update_row_lookup_cold_api() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    assert!(sql.contains("CREATE OR REPLACE FUNCTION koldstore.update_row"));
    assert!(sql.contains("lookup_cold boolean DEFAULT false"));
}
