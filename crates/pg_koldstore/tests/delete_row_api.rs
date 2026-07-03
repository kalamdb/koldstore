#[test]
fn sql_exposes_delete_row_tombstone_api() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    assert!(sql.contains("CREATE OR REPLACE FUNCTION koldstore.delete_row"));
    assert!(sql.contains("allow_may_contain boolean DEFAULT true"));
}
