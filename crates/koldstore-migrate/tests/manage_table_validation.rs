use koldstore_migrate::constraints::{
    ColumnDefinition, ManageTableConstraintsCatalog, MigrationConstraintError,
    MigrationValidationInput, UniqueConstraintShape,
};
use koldstore_migrate::manage_table::{
    validate_manage_table, ManageTablePolicyInput, ManageTableValidationContext,
};

fn valid_context() -> ManageTableValidationContext<'static> {
    ManageTableValidationContext {
        migration: MigrationValidationInput::minimal_shared(),
        already_managed: false,
        migration_order_by: None,
        compression: None,
        mirror_capture_mode: None,
        policy: ManageTablePolicyInput {
            hot_row_limit: Some(10_000),
            min_flush_rows: 1_000,
            max_rows_per_file: 1_000,
            target_file_size_mb: None,
            min_max_rows_per_file: 1_000,
        },
    }
}

#[test]
fn valid_context_returns_canonical_manage_table_options() {
    let validated = validate_manage_table(valid_context()).unwrap();

    assert_eq!(validated.options.hot_row_limit, Some(10_000));
    assert_eq!(validated.options.min_flush_rows, Some(1_000));
    assert_eq!(validated.options.max_rows_per_file, Some(1_000));
    assert_eq!(
        validated.options.compression,
        Some(koldstore_common::ParquetCompression::Zstd)
    );
}

#[test]
fn already_managed_table_is_rejected_first() {
    let mut context = valid_context();
    context.already_managed = true;
    context.migration.storage_exists = false;

    assert_eq!(
        validate_manage_table(context).unwrap_err(),
        MigrationConstraintError::AlreadyManaged
    );
}

#[test]
fn unsupported_table_type_is_rejected() {
    let mut context = valid_context();
    context.migration.table_type = "tenant".to_string();

    assert_eq!(
        validate_manage_table(context).unwrap_err(),
        MigrationConstraintError::UnsupportedTableType("tenant".to_string())
    );
}

#[test]
fn invalid_numeric_policy_values_are_rejected() {
    let mut context = valid_context();
    context.policy.hot_row_limit = Some(0);
    assert!(matches!(
        validate_manage_table(context),
        Err(MigrationConstraintError::InvalidPolicyValue {
            field: "hot_row_limit",
            ..
        })
    ));

    let mut context = valid_context();
    context.policy.target_file_size_mb = Some(-1);
    assert!(matches!(
        validate_manage_table(context),
        Err(MigrationConstraintError::InvalidPolicyValue {
            field: "target_file_size_mb",
            ..
        })
    ));

    let mut context = valid_context();
    context.policy.min_flush_rows = -1;
    assert!(matches!(
        validate_manage_table(context),
        Err(MigrationConstraintError::InvalidPolicyValue {
            field: "min_flush_rows",
            ..
        })
    ));

    let mut context = valid_context();
    context.policy.max_rows_per_file = 0;
    assert!(matches!(
        validate_manage_table(context),
        Err(MigrationConstraintError::InvalidPolicyValue {
            field: "max_rows_per_file",
            ..
        })
    ));
}

#[test]
fn max_rows_per_file_respects_the_runtime_floor() {
    let mut context = valid_context();
    context.policy.max_rows_per_file = 999;

    assert!(matches!(
        validate_manage_table(context),
        Err(MigrationConstraintError::MaxRowsPerFileBelowFloor {
            value: 999,
            minimum: 1_000
        })
    ));
}

#[test]
fn invalid_compression_is_rejected() {
    let mut context = valid_context();
    context.compression = Some("brotli");

    assert_eq!(
        validate_manage_table(context).unwrap_err(),
        MigrationConstraintError::UnsupportedCompression("brotli".to_string())
    );
}

#[test]
fn async_mirror_capture_mode_is_persisted() {
    let mut context = valid_context();
    context.mirror_capture_mode = Some("async");

    let validated = validate_manage_table(context).unwrap();
    assert_eq!(
        validated.options.mirror_capture_mode(),
        koldstore_common::MirrorCaptureMode::Async
    );
}

#[test]
fn invalid_mirror_capture_mode_is_rejected() {
    let mut context = valid_context();
    context.mirror_capture_mode = Some("eventual");

    assert_eq!(
        validate_manage_table(context).unwrap_err(),
        MigrationConstraintError::UnsupportedMirrorCaptureMode("eventual".to_string())
    );
}

#[test]
fn configured_migration_order_by_must_exist() {
    let mut context = valid_context();
    context.migration_order_by = Some("created_at");

    assert_eq!(
        validate_manage_table(context).unwrap_err(),
        MigrationConstraintError::MissingOrderColumn("created_at".to_string())
    );
}

#[test]
fn user_scope_column_must_exist() {
    let mut context = valid_context();
    context.migration.table_type = "user".to_string();
    context.migration.scope_column = Some("tenant_id".to_string());

    assert_eq!(
        validate_manage_table(context).unwrap_err(),
        MigrationConstraintError::ScopeColumnNotFound("tenant_id".to_string())
    );
}

#[test]
fn unsupported_types_are_rejected_through_migration_validation() {
    let mut context = valid_context();
    context
        .migration
        .columns
        .push(ColumnDefinition::new("network", "inet", true));

    assert!(matches!(
        validate_manage_table(context),
        Err(MigrationConstraintError::UnsupportedColumnType { column, .. })
            if column == "network"
    ));
}

#[test]
fn flush_constraint_policy_is_applied_by_the_entry_point() {
    let mut context = valid_context();
    context.migration.unique_constraints = vec![UniqueConstraintShape {
        name: "events_external_id_key".to_string(),
        columns: vec!["external_id".to_string()],
    }];

    assert!(matches!(
        validate_manage_table(context),
        Err(MigrationConstraintError::UnsupportedUniqueConstraints { .. })
    ));
}

#[test]
fn constraints_catalog_can_feed_the_migration_input() {
    let catalog = ManageTableConstraintsCatalog::default();
    let mut context = valid_context();
    context.migration.unique_constraints = catalog.unique_constraints;
    context.migration.foreign_keys = catalog.foreign_keys;

    assert!(validate_manage_table(context).is_ok());
}
