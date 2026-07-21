use koldstore_common::{
    PgCollation, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
    PrimaryKeyShape,
};
use koldstore_migrate::{mirror, register, QualifiedTableName};

fn pk_column(
    name: &str,
    ordinal: u16,
    type_oid: u32,
    type_name: &str,
    typmod: i32,
) -> PrimaryKeyColumnShape {
    PrimaryKeyColumnShape::new(
        PkColumn::new(name).unwrap(),
        PkOrdinal::new(ordinal).unwrap(),
        PgTypeOid::new(type_oid).unwrap(),
        PgTypeName::new(type_name).unwrap(),
        PgTypmod::new(typmod),
        None,
        None,
        true,
    )
}

fn plan_sql(shape: PrimaryKeyShape) -> String {
    let source = QualifiedTableName::parse("public.messages").unwrap();
    mirror::plan_change_log_mirror(&source, &shape)
        .unwrap()
        .create_table
        .sql
}

#[test]
fn mirror_preserves_single_column_primary_key_shape() {
    let sql = plan_sql(PrimaryKeyShape::new(vec![pk_column("id", 1, 20, "bigint", -1)]).unwrap());

    assert!(sql.contains("\"id\" bigint NOT NULL"));
    assert!(sql.contains("PRIMARY KEY (\"id\")"));
}

#[test]
fn mirror_preserves_composite_primary_key_order() {
    let sql = plan_sql(
        PrimaryKeyShape::new(vec![
            pk_column("tenant_id", 1, 2950, "uuid", -1),
            pk_column("id", 2, 20, "bigint", -1),
        ])
        .unwrap(),
    );

    assert!(sql.contains("\"tenant_id\" uuid NOT NULL"));
    assert!(sql.contains("\"id\" bigint NOT NULL"));
    assert!(sql.contains("PRIMARY KEY (\"tenant_id\", \"id\")"));
}

#[test]
fn mirror_preserves_typmod_collation_and_domain_identity() {
    let code = pk_column("code", 1, 1043, "character varying", 36);
    let mut locale = pk_column("locale", 2, 25, "text", -1);
    locale = PrimaryKeyColumnShape::new(
        locale.column().clone(),
        locale.ordinal(),
        locale.type_oid(),
        locale.type_name().clone(),
        locale.typmod(),
        Some(PgCollation::new("app.case_insensitive").unwrap()),
        None,
        true,
    );
    let domain = PrimaryKeyColumnShape::new(
        PkColumn::new("message_id").unwrap(),
        PkOrdinal::new(3).unwrap(),
        PgTypeOid::new(80001).unwrap(),
        PgTypeName::new("bigint").unwrap(),
        PgTypmod::new(-1),
        None,
        Some(PgTypeName::new("app.message_id").unwrap()),
        true,
    );

    let sql = plan_sql(PrimaryKeyShape::new(vec![code, locale, domain]).unwrap());

    assert!(sql.contains("\"code\" varchar(32) NOT NULL"));
    assert!(sql.contains("\"locale\" text COLLATE \"app\".\"case_insensitive\" NOT NULL"));
    assert!(sql.contains("\"message_id\" \"app\".\"message_id\" NOT NULL"));
    assert!(sql.contains("PRIMARY KEY (\"code\", \"locale\", \"message_id\")"));
}

#[test]
fn primary_key_shape_probe_reads_exact_catalog_metadata() {
    let probe = register::primary_key_shape_probe_plan(42).unwrap();

    assert_eq!(probe.operation, "capture primary-key shape");
    assert!(probe.sql.contains("pg_index"));
    assert!(probe.sql.contains("pg_attribute"));
    assert!(probe.sql.contains("pg_type"));
    assert!(probe.sql.contains("pg_collation"));
    assert!(probe.sql.contains("collisdeterministic"));
    assert!(probe.sql.contains("domain_identity"));
    assert!(probe.sql.contains("$1::oid"));
}

#[test]
fn nondeterministic_primary_key_collation_is_rejected() {
    let error =
        register::primary_key_shape_from_catalog_rows(vec![register::PrimaryKeyShapeCatalogRow {
            column: "id".to_string(),
            ordinal: 1,
            type_oid: 25,
            type_name: "text".to_string(),
            typmod: -1,
            collation: Some("app.case_insensitive".to_string()),
            collation_deterministic: Some(false),
            domain_identity: None,
            not_null: true,
        }])
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "primary-key column `id` uses unsupported nondeterministic collation `app.case_insensitive`"
    );
}

#[test]
fn mirror_rejects_missing_primary_key_before_artifact_creation() {
    let source = QualifiedTableName::parse("public.messages").unwrap();
    let empty = PrimaryKeyShape::new(Vec::new());

    assert!(empty.is_err());
    assert!(mirror::plan_change_log_mirror_from_columns(&source, &[]).is_err());
}
