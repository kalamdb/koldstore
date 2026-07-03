use arrow::datatypes::DataType;
use koldstore_parquet::{build_arrow_schema, PgColumn, PgType, SchemaError, SystemColumn};

#[test]
fn postgres_schema_conversion_adds_system_columns_and_supported_types() {
    let schema = build_arrow_schema(&[
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
        schema.field_with_name("_seq").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("_commit_seq").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("_deleted").unwrap().data_type(),
        &DataType::Boolean
    );
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["id", "body", "_seq", "_commit_seq", "_deleted"]
    );
}

#[test]
fn system_columns_have_contract_names() {
    assert_eq!(SystemColumn::Seq.name(), "_seq");
    assert_eq!(SystemColumn::CommitSeq.name(), "_commit_seq");
    assert_eq!(SystemColumn::Deleted.name(), "_deleted");
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
        ("uuid", DataType::Utf8),
        ("jsonb", DataType::Utf8),
        (
            "timestamptz",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        (
            "timestamp with time zone",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
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
    let error = PgColumn::from_catalog("payload", "bytea", true).unwrap_err();

    assert_eq!(error, SchemaError::UnsupportedType("bytea".to_string()));
    assert!(PgColumn::from_catalog("amount", "numeric", false).is_err());
    assert!(PgColumn::from_catalog("addr", "inet", true).is_err());
}
