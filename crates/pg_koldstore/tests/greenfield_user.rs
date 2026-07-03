#[test]
fn sql_extension_exposes_user_scoped_migration_contract() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    for needle in [
        "table_type NOT IN ('shared', 'user')",
        "scope_column IS NULL",
        "koldstore.user_id",
        "ALTER TABLE %s ADD COLUMN IF NOT EXISTS _user_id text",
        "user-scoped tables require a scope column or system _user_id",
    ] {
        assert!(
            sql.contains(needle),
            "missing user-scope SQL fragment: {needle}"
        );
    }
}
