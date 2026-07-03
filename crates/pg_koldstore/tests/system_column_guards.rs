use pg_koldstore::hooks::executor;

#[test]
fn direct_system_column_writes_require_internal_guard() {
    assert!(executor::rejects_system_column_write("_seq", false));
    assert!(executor::rejects_system_column_write("_commit_seq", false));
    assert!(executor::rejects_system_column_write("_deleted", false));
    assert!(!executor::rejects_system_column_write("_seq", true));
    assert!(!executor::rejects_system_column_write("body", false));
}
