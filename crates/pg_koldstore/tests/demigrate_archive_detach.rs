#[test]
fn archive_detach_mode_warns_about_cold_only_rows() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");
    assert!(sql.contains("archive-detach mode"));
    assert!(sql.contains("cold-only rows will not be visible"));
}
