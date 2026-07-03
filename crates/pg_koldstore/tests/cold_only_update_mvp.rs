#[test]
fn standard_sql_cold_only_update_is_documented_as_out_of_mvp() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    assert!(sql.contains("standard SQL cold-only UPDATE affects 0 rows in MVP"));
}
