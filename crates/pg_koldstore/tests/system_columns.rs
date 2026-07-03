use pg_koldstore::migrate::columns::{
    plan_system_column_adds, system_columns, REQUIRED_SYSTEM_COLUMNS,
};
use pg_koldstore::migrate::QualifiedTableName;
use pg_koldstore::spi::SpiAccess;

#[test]
fn shared_system_column_plan_preserves_primary_key_shape() {
    let table = QualifiedTableName::parse("app.items").unwrap();
    let plan = plan_system_column_adds(&table, false).unwrap();

    assert_eq!(plan.columns, REQUIRED_SYSTEM_COLUMNS);
    assert_eq!(plan.statement.operation, "add system columns");
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan
        .statement
        .sql
        .contains("ALTER TABLE ONLY \"app\".\"items\""));
    assert!(plan
        .statement
        .sql
        .contains("ADD COLUMN IF NOT EXISTS \"_seq\" bigint NOT NULL"));
    assert!(plan
        .statement
        .sql
        .contains("ADD COLUMN IF NOT EXISTS \"_commit_seq\" bigint NOT NULL"));
    assert!(plan
        .statement
        .sql
        .contains("ADD COLUMN IF NOT EXISTS \"_deleted\" boolean NOT NULL"));
    assert!(!plan.statement.sql.contains("\"_user_id\""));
    assert!(!plan.statement.sql.contains("PRIMARY KEY"));
    assert!(!plan.statement.sql.contains("DROP CONSTRAINT"));
}

#[test]
fn user_system_column_plan_adds_internal_scope_when_needed() {
    let table = QualifiedTableName::parse("notes").unwrap();
    let plan = plan_system_column_adds(&table, true).unwrap();

    assert_eq!(
        plan.columns,
        ["_seq", "_commit_seq", "_deleted", "_user_id"]
    );
    assert_eq!(
        system_columns(true),
        vec!["_seq", "_commit_seq", "_deleted", "_user_id"]
    );
    assert!(plan.statement.sql.contains("ALTER TABLE ONLY \"notes\""));
    assert!(plan
        .statement
        .sql
        .contains("ADD COLUMN IF NOT EXISTS \"_user_id\" text"));
}
