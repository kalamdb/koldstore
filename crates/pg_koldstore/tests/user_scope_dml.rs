use pg_koldstore::security::scope;

#[test]
fn cross_scope_dml_is_denied() {
    assert!(scope::scope_matches("user-a", "user-a"));
    assert!(!scope::scope_matches("user-a", "user-b"));
}
