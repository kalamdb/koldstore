#[test]
fn migration_sql_enforces_fk_hot_only_policy() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    for needle in [
        "allow_fk_hot_only",
        "pg_constraint",
        "contype = 'f'",
        "FK constraints are hot-only when flush is enabled",
    ] {
        assert!(sql.contains(needle), "missing FK policy fragment: {needle}");
    }
}
