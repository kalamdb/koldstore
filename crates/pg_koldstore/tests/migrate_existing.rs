use koldstore_core::{PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape};
use pg_koldstore::{
    migrate::{
        backfill::plan_mirror_initialization_batch,
        constraints::{ColumnDefinition, IndexDefinition, MigrationValidationInput},
        jobs::MigrationBatchSize,
        order::{MigrationOrdering, OrderingSource},
        QualifiedTableName,
    },
    spi::{SpiAccess, SqlParamType},
};

fn pk() -> Vec<PrimaryKeyColumnShape> {
    vec![PrimaryKeyColumnShape::new(
        PkColumn::new("id").unwrap(),
        PkOrdinal::new(1).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("bigint").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    )]
}

fn ordering() -> MigrationOrdering {
    MigrationOrdering {
        column: "id".to_string(),
        source: OrderingSource::AutoIncrementPrimaryKey,
        ascending_oldest_first: true,
    }
}

#[test]
fn migration_sql_initializes_existing_rows_into_mirror_in_ordered_batches() {
    let plan = plan_mirror_initialization_batch(
        &QualifiedTableName::parse("app.items").unwrap(),
        &QualifiedTableName::parse("koldstore.items__cl").unwrap(),
        &pk(),
        ordering(),
        MigrationBatchSize::new(10_000).unwrap(),
    )
    .unwrap();

    assert!(plan.statement.sql.contains("LIMIT $1"));
    assert!(plan
        .statement
        .sql
        .contains("FOR KEY SHARE OF hot SKIP LOCKED"));
    assert!(plan
        .statement
        .sql
        .contains("ORDER BY hot.\"id\" ASC, hot.ctid ASC"));
    assert!(plan
        .statement
        .sql
        .contains("ON CONFLICT (\"id\") DO NOTHING"));
    assert!(plan.statement.sql.contains("SNOWFLAKE_ID()"));
    assert_eq!(plan.statement.param_types, vec![SqlParamType::BigInt]);
}

#[test]
fn migration_validation_preserves_existing_table_shape() {
    let mut input = MigrationValidationInput::minimal_shared();
    input
        .columns
        .push(ColumnDefinition::new("title", "text", false));
    input.indexes.push(IndexDefinition::btree(
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
fn existing_row_initialization_uses_skip_locked_batches_not_table_wide_update() {
    let table = QualifiedTableName::parse("app.items").unwrap();
    let mirror = QualifiedTableName::parse("koldstore.items__cl").unwrap();
    let plan = plan_mirror_initialization_batch(
        &table,
        &mirror,
        &pk(),
        ordering(),
        MigrationBatchSize::new(1_000).unwrap(),
    )
    .unwrap();

    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan
        .statement
        .sql
        .contains("INSERT INTO \"koldstore\".\"items__cl\""));
    assert!(plan
        .statement
        .sql
        .contains("WITH candidate AS MATERIALIZED"));
    assert!(plan.statement.sql.contains("LIMIT $1"));
    assert!(plan
        .statement
        .sql
        .contains("FOR KEY SHARE OF hot SKIP LOCKED"));
    assert!(!plan.statement.sql.contains("UPDATE ONLY \"app\".\"items\""));
    for forbidden in [
        "\"_seq\"",
        "\"_commit_seq\"",
        "\"_deleted\"",
        "\"_user_id\"",
    ] {
        assert!(!plan.statement.sql.contains(forbidden));
    }
}
