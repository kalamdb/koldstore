use pg_koldstore::security::scope;

#[test]
fn missing_user_scope_fails_closed() {
    assert!(scope::require_user_scope(None).is_err());
    assert_eq!(
        scope::require_user_scope(Some(" user-a ")).unwrap(),
        "user-a"
    );
}
