#[test]
fn migration_sql_rejects_unsupported_generated_and_expression_shapes() {
    for supported in [
        "bool",
        "boolean",
        "int2",
        "smallint",
        "int4",
        "integer",
        "int8",
        "bigint",
        "float4",
        "real",
        "float8",
        "double precision",
        "text",
        "varchar",
        "character varying",
        "numeric",
        "uuid",
        "jsonb",
        "bytea",
        "timestamptz",
        "timestamp with time zone",
    ] {
        assert!(
            koldstore_migrate::constraints::type_supported(supported),
            "{supported} should be supported"
        );
    }
    for unsupported in ["inet", "geometry", "tsvector", "cidr"] {
        assert!(
            !koldstore_migrate::constraints::type_supported(unsupported),
            "{unsupported} should be unsupported"
        );
    }
}

#[test]
fn migration_validation_rejects_unsupported_generated_and_expression_shapes() {
    let mut input = koldstore_migrate::constraints::MigrationValidationInput::minimal_shared();
    input
        .columns
        .push(koldstore_migrate::constraints::ColumnDefinition::new(
            "search", "tsvector", true,
        ));
    assert!(input.validate().is_err());

    input = koldstore_migrate::constraints::MigrationValidationInput::minimal_shared();
    input.columns[0].generated = true;
    assert!(input.validate().is_err());

    input = koldstore_migrate::constraints::MigrationValidationInput::minimal_shared();
    input.expression_primary_key = true;
    assert!(input.validate().is_err());

    input = koldstore_migrate::constraints::MigrationValidationInput::minimal_shared();
    input
        .indexes
        .push(koldstore_migrate::constraints::IndexDefinition::expression(
            "lower_title_idx",
        ));
    assert!(input.validate().is_err());
}
