#[test]
fn migration_sql_backfills_existing_rows_in_ordered_batches() {
    use pg_koldstore::migrate::jobs::{backfill_batch_plan, MigrationBatchSize};
    use pg_koldstore::migrate::order::{MigrationOrdering, OrderingSource};
    use pg_koldstore::migrate::QualifiedTableName;

    let plan = backfill_batch_plan(
        &QualifiedTableName::parse("app.items").unwrap(),
        MigrationOrdering {
            column: "id".to_string(),
            source: OrderingSource::AutoIncrementPrimaryKey,
            ascending_oldest_first: true,
        },
        MigrationBatchSize::new(10_000).unwrap(),
    )
    .unwrap();

    assert!(plan.statement.sql.contains("LIMIT $1"));
    assert!(plan.statement.sql.contains("FOR UPDATE SKIP LOCKED"));
    assert!(plan.statement.sql.contains("ORDER BY \"id\" ASC, ctid ASC"));
    assert!(plan
        .statement
        .sql
        .contains("nextval('koldstore.global_seq'::regclass) AS assigned_seq"));
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
fn existing_row_backfill_plan_uses_skip_locked_batches_not_table_wide_update() {
    use pg_koldstore::migrate::jobs::{backfill_batch_plan, MigrationBatchSize};
    use pg_koldstore::migrate::order::{MigrationOrdering, OrderingSource};
    use pg_koldstore::migrate::QualifiedTableName;
    use pg_koldstore::spi::SpiAccess;

    let table = QualifiedTableName::parse("app.items").unwrap();
    let plan = backfill_batch_plan(
        &table,
        MigrationOrdering {
            column: "id".to_string(),
            source: OrderingSource::AutoIncrementPrimaryKey,
            ascending_oldest_first: true,
        },
        MigrationBatchSize::new(1_000).unwrap(),
    )
    .unwrap();

    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan.statement.sql.contains("UPDATE ONLY \"app\".\"items\""));
    assert!(plan
        .statement
        .sql
        .contains("WITH candidate AS MATERIALIZED"));
    assert!(plan.statement.sql.contains("LIMIT $1"));
    assert!(plan.statement.sql.contains("FOR UPDATE SKIP LOCKED"));
    assert!(plan.statement.sql.contains("AND hot.\"_seq\" IS NULL"));
    assert!(!plan.statement.sql.contains("SNOWFLAKE_ID()"));
}
