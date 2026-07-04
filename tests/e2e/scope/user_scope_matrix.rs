#[path = "../common/mod.rs"]
mod common;

use koldstore_core::TableKind;

#[test]
fn user_scope_matrix_targets_postgresql_15_16_17() {
    assert_eq!(
        common::local_pg_matrix()
            .into_iter()
            .map(|target| target.version)
            .collect::<Vec<_>>(),
        common::expected_pg_versions()
    );
}

#[test]
fn user_scope_matrix_contract_covers_missing_scope_and_cross_scope_denial() {
    let missing =
        pg_koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, None).unwrap_err();
    assert_eq!(missing.to_string(), "koldstore.user_id is not set");

    let planned =
        pg_koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, Some("user-a"))
            .unwrap()
            .unwrap();
    let row_scope = koldstore_core::ScopeKey::new("user-b").unwrap();
    let denied = pg_koldstore::hooks::executor::enforce_dml_scope(
        TableKind::User,
        Some(planned.as_str()),
        Some(&row_scope),
    )
    .unwrap_err();

    assert_eq!(
        denied.to_string(),
        "row scope `user-b` does not match koldstore.user_id `user-a`"
    );
}
