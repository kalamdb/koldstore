#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use koldstore_common::TableKind;

#[test]
fn user_scope_matrix_targets_active_pgrx_versions() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

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
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let missing =
        pg_koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, None).unwrap_err();
    assert_eq!(missing.to_string(), "koldstore.user_id is not set");

    let planned =
        pg_koldstore::hooks::planner::plan_scope_key_for_read(TableKind::User, Some("user-a"))
            .unwrap()
            .unwrap();
    let row_scope = koldstore_common::ScopeKey::new("user-b").unwrap();
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

#[tokio::test]
async fn user_scope_migration_installs_fail_closed_policy_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "user_scope_matrix").await?;
        let table = db.create_user_notes_table("scope_matrix_notes").await?;
        db.migrate_user_scoped(&table.relation, "user_id").await?;

        let schema_row = db
            .client
            .query_one(
                r#"
                SELECT table_type, scope_column
                FROM koldstore.schemas
                WHERE table_oid = $1::text::regclass::oid
                  AND active
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(schema_row.get::<_, String>(0), "user");
        assert_eq!(
            schema_row.get::<_, Option<String>>(1).as_deref(),
            Some("user_id")
        );

        let policy_row = db
            .client
            .query_one(
                r#"
                SELECT c.relrowsecurity, count(p.policyname)::bigint
                FROM pg_class c
                LEFT JOIN pg_policies p
                  ON p.schemaname = $2
                 AND p.tablename = $3
                 AND p.policyname = 'koldstore_user_scope_fail_closed'
                WHERE c.oid = $1::text::regclass
                GROUP BY c.relrowsecurity
                "#,
                &[&table.relation, &db.schema, &table.table_name],
            )
            .await?;
        assert!(policy_row.get::<_, bool>(0));
        assert_eq!(policy_row.get::<_, i64>(1), 1);

        db.client
            .batch_execute("SET koldstore.user_id = 'user-a'")
            .await?;
        let active_scope = db
            .client
            .query_one("SELECT current_setting('koldstore.user_id', false)", &[])
            .await?
            .get::<_, String>(0);
        assert_eq!(active_scope, "user-a");
    }

    Ok(())
}
