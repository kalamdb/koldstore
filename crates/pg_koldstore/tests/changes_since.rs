#[test]
fn sql_exposes_changes_since_ordered_by_commit_seq() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    assert!(sql.contains("CREATE OR REPLACE FUNCTION koldstore.changes_since"));
    assert!(sql.contains("ORDER BY commit_seq"));
    assert!(sql.contains("retention gap"));
}
