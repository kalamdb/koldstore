#[test]
fn migration_sql_backfills_existing_rows_and_preserves_primary_key() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    for needle in [
        "Migration MUST NOT rewrite the primary key",
        "UPDATE %s SET _seq = COALESCE(_seq, SNOWFLAKE_ID())",
        "_commit_seq = COALESCE(_commit_seq, nextval('koldstore.global_commit_seq'::regclass))",
        "_deleted = COALESCE(_deleted, false)",
    ] {
        assert!(
            sql.contains(needle),
            "missing existing migration fragment: {needle}"
        );
    }
}
