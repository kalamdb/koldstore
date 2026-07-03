#[test]
fn demigration_sql_deactivates_managed_metadata() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    assert!(sql.contains("SET active = false"));
    assert!(sql.contains("demigration disables KoldstoreMergeScan"));
}
