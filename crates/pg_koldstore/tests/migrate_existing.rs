#[test]
fn migration_sql_backfills_existing_rows_and_preserves_primary_key() {
    let plan = pg_koldstore::migrate::backfill::BackfillPlan::new("app.items");
    let sql = plan.sql();

    for needle in [
        "UPDATE app.items SET _seq = COALESCE(_seq, SNOWFLAKE_ID())",
        "_commit_seq = COALESCE(_commit_seq, nextval('koldstore.global_commit_seq'::regclass))",
        "_deleted = COALESCE(_deleted, false)",
    ] {
        assert!(
            sql.contains(needle),
            "missing existing migration fragment: {needle}"
        );
    }
}

#[test]
fn migration_validation_preserves_existing_table_shape() {
    let mut input = pg_koldstore::migrate::constraints::MigrationValidationInput::minimal_shared();
    input
        .columns
        .push(pg_koldstore::migrate::constraints::ColumnDefinition::new(
            "title", "text", false,
        ));
    input
        .indexes
        .push(pg_koldstore::migrate::constraints::IndexDefinition::btree(
            "items_title_idx",
            vec!["title".to_string()],
        ));
    input.check_constraints = vec!["title <> ''".to_string()];
    input.not_null_columns = vec!["id".to_string(), "title".to_string()];

    let validation = input.validate().unwrap();

    assert_eq!(validation.primary_key, vec!["id"]);
    assert_eq!(validation.indexed_columns, vec!["title"]);
    assert_eq!(validation.preserved_indexes, vec!["items_title_idx"]);
    assert_eq!(validation.preserved_check_constraints, vec!["title <> ''"]);
    assert_eq!(validation.preserved_not_null_columns, vec!["id", "title"]);
    assert!(
        !validation.primary_key.iter().any(|column| column == "_seq"),
        "migration must not rewrite the application primary key to include _seq"
    );
}

#[test]
fn existing_row_backfill_plan_locks_and_updates_only_missing_system_values() {
    use pg_koldstore::migrate::backfill::plan_existing_row_backfill;
    use pg_koldstore::migrate::QualifiedTableName;
    use pg_koldstore::spi::SpiAccess;

    let table = QualifiedTableName::parse("app.items").unwrap();
    let plan = plan_existing_row_backfill(&table, 42).unwrap();

    assert_eq!(plan.table_oid, 42);
    assert_eq!(plan.statements.len(), 2);
    assert!(plan
        .statements
        .iter()
        .all(|statement| statement.access == SpiAccess::ReadWrite));
    assert_eq!(
        plan.statements[0].sql,
        "SELECT pg_advisory_xact_lock($1, $2)"
    );
    assert!(plan.statements[1]
        .sql
        .contains("UPDATE ONLY \"app\".\"items\""));
    assert!(plan.statements[1]
        .sql
        .contains("\"_seq\" = COALESCE(\"_seq\", SNOWFLAKE_ID())"));
    assert!(plan.statements[1].sql.contains(
        "\"_commit_seq\" = COALESCE(\"_commit_seq\", nextval('koldstore.global_commit_seq'::regclass))"
    ));
    assert!(plan.statements[1]
        .sql
        .contains("\"_deleted\" = COALESCE(\"_deleted\", false)"));
    assert!(plan.statements[1].sql.contains("WHERE \"_seq\" IS NULL"));
}
