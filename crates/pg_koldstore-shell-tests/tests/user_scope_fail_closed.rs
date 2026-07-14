use koldstore_common::scope;
use koldstore_common::{ScopeKey, TableKind};

#[test]
fn missing_user_scope_fails_closed() {
    assert!(scope::require_user_scope(None).is_err());
    assert_eq!(
        scope::require_user_scope(Some(" user-a ")).unwrap(),
        "user-a"
    );
}

#[test]
fn planner_requires_scope_before_user_table_read() {
    assert_eq!(
        koldstore::hooks::planner::plan_scope_key_for_read(TableKind::Shared, None).unwrap(),
        None
    );

    let missing =
        koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, None).unwrap_err();
    assert_eq!(missing.to_string(), "koldstore.user_id is not set");

    let planned =
        koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, Some(" user-a "))
            .unwrap();
    assert_eq!(planned, Some(ScopeKey::new("user-a").unwrap()));
}
