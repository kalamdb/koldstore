#[test]
fn migration_sql_rejects_tables_without_primary_key() {
    assert!(!koldstore_migrate::constraints::primary_key_shape_supported(&[]));
    assert!(!koldstore_migrate::constraints::primary_key_shape_supported(&[""]));
    assert!(koldstore_migrate::constraints::primary_key_shape_supported(
        &["id"]
    ));
}

#[test]
fn migration_validation_requires_named_primary_key_columns() {
    let mut input = koldstore_migrate::constraints::MigrationValidationInput::minimal_shared();
    input.primary_key.clear();
    assert!(input.validate().is_err());

    input.primary_key = vec![" ".to_string()];
    assert!(input.validate().is_err());

    input.primary_key = vec!["missing".to_string()];
    assert!(input.validate().is_err());

    input.primary_key = vec!["id".to_string()];
    assert_eq!(input.validate().unwrap().primary_key, vec!["id"]);
}
