#[test]
fn sql_contains_pending_write_manifest_state_for_hot_dml() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    assert!(sql.contains("pending_write"));
    assert!(sql.contains("normal DML does not rewrite object-store manifests"));
}
