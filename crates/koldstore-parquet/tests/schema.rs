use arrow::datatypes::DataType;
use koldstore_parquet::{build_arrow_schema, PgColumn, PgType, SystemColumn};

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
}

#[test]
fn system_columns_have_contract_names() {
    assert_eq!(SystemColumn::Seq.name(), "_seq");
    assert_eq!(SystemColumn::CommitSeq.name(), "_commit_seq");
    assert_eq!(SystemColumn::Deleted.name(), "_deleted");
}
