use koldstore_common::SqlAccess as SpiAccess;
use koldstore_migrate::lock::plan_migration_operation_lock;
use koldstore_migrate::QualifiedTableName;

#[test]
fn migration_operation_lock_blocks_concurrent_table_conversion_work() {
    let table = QualifiedTableName::parse("app.items").unwrap();
    let plan = plan_migration_operation_lock(&table, 42).unwrap();

    assert_eq!(plan.table_oid, 42);
    assert_eq!(plan.statements.len(), 2);
    assert!(plan
        .statements
        .iter()
        .all(|statement| statement.access == SpiAccess::ReadWrite));
    assert_eq!(plan.statements[0].sql, "SELECT pg_advisory_xact_lock($1)");
    assert_eq!(plan.lock_key.as_advisory_lock_key(), 0x6b6f466473746f);
    assert_eq!(
        plan.statements[1].sql,
        "LOCK TABLE ONLY \"app\".\"items\" IN ACCESS EXCLUSIVE MODE"
    );
}

#[test]
fn migration_operation_lock_rejects_missing_table_oid() {
    let table = QualifiedTableName::parse("app.items").unwrap();

    assert!(plan_migration_operation_lock(&table, 0).is_err());
}
