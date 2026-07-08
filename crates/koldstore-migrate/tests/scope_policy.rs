use koldstore_common::SqlAccess as SpiAccess;
use koldstore_migrate::scope::plan_user_scope_policy;
use koldstore_migrate::QualifiedTableName;

#[test]
fn user_scope_policy_plan_enables_fail_closed_rls() {
    let table = QualifiedTableName::parse("app.notes").unwrap();
    let plan = plan_user_scope_policy(&table, "user_id").unwrap();

    assert_eq!(plan.scope_column, "user_id");
    assert_eq!(plan.statements.len(), 3);
    assert!(plan
        .statements
        .iter()
        .all(|statement| statement.access == SpiAccess::ReadWrite));
    assert_eq!(
        plan.statements[0].sql,
        "ALTER TABLE ONLY \"app\".\"notes\" ENABLE ROW LEVEL SECURITY"
    );
    assert_eq!(
        plan.statements[1].sql,
        "DROP POLICY IF EXISTS koldstore_user_scope_fail_closed ON \"app\".\"notes\""
    );

    let create_policy = &plan.statements[2].sql;
    assert!(create_policy.contains("CREATE POLICY koldstore_user_scope_fail_closed"));
    assert!(create_policy.contains("FOR ALL"));
    assert!(create_policy.contains("current_setting('koldstore.user_id', true) IS NOT NULL"));
    assert!(create_policy.contains("\"user_id\" = current_setting('koldstore.user_id', true)"));
    assert!(create_policy.contains("WITH CHECK"));
}

#[test]
fn user_scope_policy_plan_rejects_unsafe_scope_columns() {
    let table = QualifiedTableName::parse("app.notes").unwrap();

    assert!(plan_user_scope_policy(&table, "").is_err());
    assert!(plan_user_scope_policy(&table, "not safe").is_err());
    assert!(plan_user_scope_policy(&table, "user_id; drop table app.notes").is_err());
}
