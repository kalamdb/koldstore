use koldstore_common::{
    ColumnId, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
    PrimaryKeyShape,
};
use koldstore_migrate::{mirror, QualifiedTableName};
use koldstore_wal_mirror::plan_mirror_pk_guard;

fn pk_column(name: &str, ordinal: u16) -> PrimaryKeyColumnShape {
    PrimaryKeyColumnShape::new(
        ColumnId::from_attnum(ordinal as i16),
        PkColumn::new(name).unwrap(),
        PkOrdinal::new(ordinal).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("bigint").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    )
}

#[test]
fn change_log_mirror_installs_pk_guard_only() {
    let source = QualifiedTableName::parse("public.messages").unwrap();
    let shape = PrimaryKeyShape::new(vec![pk_column("tenant_id", 1), pk_column("id", 2)]).unwrap();
    let plan = mirror::plan_change_log_mirror(&source, &shape).unwrap();
    let create_sql = plan
        .create_statements()
        .into_iter()
        .map(|statement| statement.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(create_sql.contains("CREATE TABLE"));
    assert!(create_sql.contains("pk_update_guard") || create_sql.contains("_pk_guard"));
    assert!(!create_sql.contains("_insert_capture"));
    assert!(!create_sql.contains("_update_capture"));
    assert!(!create_sql.contains("_delete_capture"));

    let guard = plan_mirror_pk_guard(&source, &plan.mirror_table, shape.columns(), None).unwrap();
    assert!(guard
        .trigger
        .sql
        .contains("BEFORE UPDATE OF \"tenant_id\", \"id\""));
    assert!(guard.trigger.sql.contains("FOR EACH ROW"));
    assert!(
        !guard.trigger.sql.contains("DROP TRIGGER IF EXISTS"),
        "create path must avoid NOTICE-emitting DROP TRIGGER IF EXISTS"
    );
    assert!(guard.trigger.sql.contains("$koldstore_drop_trigger$"));
    assert!(guard.trigger.sql.contains(
        "EXECUTE 'DROP TRIGGER \"messages__cl_pk_update_guard\" ON \"public\".\"messages\"'"
    ));
}

#[test]
fn change_log_mirror_create_statements_include_schema_and_guard() {
    let source = QualifiedTableName::parse("public.messages").unwrap();
    let shape = PrimaryKeyShape::new(vec![pk_column("id", 1)]).unwrap();
    let plan = mirror::plan_change_log_mirror(&source, &shape).unwrap();
    assert_eq!(plan.create_statements().len(), 5);
}
