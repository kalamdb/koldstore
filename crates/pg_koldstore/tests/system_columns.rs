use koldstore_common::{
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
};
use koldstore_migrate::{mirror::plan_change_log_mirror_from_columns, QualifiedTableName};

#[test]
fn clean_schema_migration_uses_mirror_table_instead_of_system_columns() {
    let source = QualifiedTableName::parse("app.items").unwrap();
    let pk = PrimaryKeyColumnShape::new(
        PkColumn::new("id").unwrap(),
        PkOrdinal::new(1).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("bigint").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    );

    let plan = plan_change_log_mirror_from_columns(&source, &[pk]).unwrap();

    assert!(plan
        .create_table
        .sql
        .contains("CREATE TABLE IF NOT EXISTS \"koldstore\".\"items__cl\""));
    for forbidden in [
        "\"_seq\"",
        "\"_commit_seq\"",
        "\"_deleted\"",
        "\"_user_id\"",
    ] {
        assert!(!plan.create_table.sql.contains(forbidden));
    }
}
