use koldstore_common::scope;
use koldstore_common::{ScopeKey, TableKind};

#[test]
fn cross_scope_dml_is_denied() {
    assert!(scope::scope_matches("user-a", "user-a"));
    assert!(!scope::scope_matches("user-a", "user-b"));
}

#[test]
fn dml_scope_checks_run_before_heap_or_cold_metadata_access() {
    let active = ScopeKey::new("user-a").unwrap();
    let row = ScopeKey::new("user-a").unwrap();
    let other_row = ScopeKey::new("user-b").unwrap();

    assert_eq!(
        pg_koldstore::hooks::executor::enforce_dml_scope(TableKind::Shared, None, None,).unwrap(),
        None
    );

    assert_eq!(
        pg_koldstore::hooks::executor::enforce_dml_scope(
            TableKind::User,
            Some(active.as_str()),
            Some(&row),
        )
        .unwrap(),
        Some(active.clone())
    );

    let missing_session =
        pg_koldstore::hooks::executor::enforce_dml_scope(TableKind::User, None, Some(&row))
            .unwrap_err();
    assert_eq!(missing_session.to_string(), "koldstore.user_id is not set");

    let missing_row = pg_koldstore::hooks::executor::enforce_dml_scope(
        TableKind::User,
        Some(active.as_str()),
        None,
    )
    .unwrap_err();
    assert_eq!(missing_row.to_string(), "row scope is missing");

    let cross_scope = pg_koldstore::hooks::executor::enforce_dml_scope(
        TableKind::User,
        Some(active.as_str()),
        Some(&other_row),
    )
    .unwrap_err();
    assert_eq!(
        cross_scope.to_string(),
        "row scope `user-b` does not match koldstore.user_id `user-a`"
    );
}
