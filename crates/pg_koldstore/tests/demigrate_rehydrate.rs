#[test]
fn sql_exposes_demigrate_table_with_rehydrate_default() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    assert!(sql.contains("CREATE OR REPLACE FUNCTION koldstore.demigrate_table"));
    assert!(sql.contains("rehydrate boolean DEFAULT true"));
}
