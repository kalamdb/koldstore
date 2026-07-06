use arrow_schema::{DataType, TimeUnit};
use koldstore_parquet::{
    build_clean_arrow_schema, ColdMetadataColumn, PgColumn, PgType, SchemaError,
};

#[test]
fn clean_schema_conversion_adds_mirror_metadata_not_user_table_system_columns() {
    let schema = build_clean_arrow_schema(&[
        PgColumn::new("id", PgType::Int8, false),
        PgColumn::new("body", PgType::Text, true),
    ])
    .unwrap();

    assert_eq!(
        schema.field_with_name("id").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("body").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec![
            "id",
            "body",
            "seq",
            "op",
            "changed_at",
            "deleted",
            "schema_version"
        ]
    );
    assert_eq!(
        schema.field_with_name("seq").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("op").unwrap().data_type(),
        &DataType::Int16
    );
    assert_eq!(
        schema.field_with_name("changed_at").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        schema.field_with_name("deleted").unwrap().data_type(),
        &DataType::Boolean
    );
    for forbidden in ["_seq", "_commit_seq", "_deleted"] {
        assert!(schema.field_with_name(forbidden).is_err());
    }
}

#[test]
fn cold_metadata_columns_have_clean_contract_names() {
    assert_eq!(ColdMetadataColumn::Seq.name(), "seq");
    assert_eq!(ColdMetadataColumn::Op.name(), "op");
    assert_eq!(ColdMetadataColumn::ChangedAt.name(), "changed_at");
    assert_eq!(ColdMetadataColumn::Deleted.name(), "deleted");
    assert_eq!(ColdMetadataColumn::SchemaVersion.name(), "schema_version");
}

#[test]
fn postgres_type_parser_supports_mvp_types_and_common_catalog_aliases() {
    for (type_name, expected_arrow_type) in [
        ("bool", DataType::Boolean),
        ("boolean", DataType::Boolean),
        ("int2", DataType::Int16),
        ("smallint", DataType::Int16),
        ("int4", DataType::Int32),
        ("integer", DataType::Int32),
        ("int8", DataType::Int64),
        ("bigint", DataType::Int64),
        ("float4", DataType::Float32),
        ("real", DataType::Float32),
        ("float8", DataType::Float64),
        ("double precision", DataType::Float64),
        ("text", DataType::Utf8),
        ("varchar", DataType::Utf8),
        ("character varying", DataType::Utf8),
        ("character varying(255)", DataType::Utf8),
        ("numeric", DataType::Utf8),
        ("numeric(18, 4)", DataType::Utf8),
        ("uuid", DataType::Utf8),
        ("jsonb", DataType::Utf8),
        ("text[]", DataType::Utf8),
        ("bytea", DataType::Utf8),
        (
            "timestamptz",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            "timestamp with time zone",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
    ] {
        assert_eq!(
            PgType::from_postgres_name(type_name).unwrap().arrow_type(),
            expected_arrow_type,
            "{type_name} should map to the expected Arrow type"
        );
    }
}

#[test]
fn postgres_catalog_column_conversion_rejects_unsupported_types() {
    let error = PgColumn::from_catalog("payload", "inet", true).unwrap_err();

    assert_eq!(error, SchemaError::UnsupportedType("inet".to_string()));
    assert!(PgColumn::from_catalog("amount", "numeric", false).is_ok());
    assert!(PgColumn::from_catalog("payload", "bytea", true).is_ok());
}
