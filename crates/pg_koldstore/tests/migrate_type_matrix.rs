#[test]
fn migration_sql_rejects_unsupported_generated_and_expression_shapes() {
    let sql = include_str!("../../../sql/koldstore--0.1.0.sql");

    for needle in [
        "unsupported PostgreSQL type",
        "att.attgenerated <> ''",
        "expression primary keys are not supported",
        "key_position.attnum = 0",
    ] {
        assert!(
            sql.contains(needle),
            "missing migration validation fragment: {needle}"
        );
    }
}
