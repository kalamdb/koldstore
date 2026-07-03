use pg_koldstore::hooks::executor;

#[test]
fn direct_system_column_writes_require_internal_guard() {
    for column in ["_seq", "_commit_seq", "_deleted", "_user_id"] {
        assert!(
            executor::rejects_system_column_write(column, false),
            "{column} must be guarded for user DML"
        );
        assert!(
            !executor::rejects_system_column_write(column, true),
            "{column} must be writable only under the internal guard"
        );
    }

    assert!(!executor::rejects_system_column_write("body", false));
    assert_eq!(
        executor::managed_dml_hook_names(),
        ["INSERT", "UPDATE", "DELETE", "COPY"]
    );
}
