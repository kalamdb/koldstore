use koldstore_migrate::constraints::{
    FkDirection, ForeignKeyShape, ManageTableConstraintsCatalog, MigrationValidationInput,
    UniqueConstraintShape,
};

#[test]
fn flush_enabled_tables_reject_non_primary_unique_constraints_with_column_details() {
    let catalog = ManageTableConstraintsCatalog {
        unique_constraints: vec![
            UniqueConstraintShape {
                name: "users_email_key".to_string(),
                columns: vec!["email".to_string()],
            },
            UniqueConstraintShape {
                name: "users_username_idx".to_string(),
                columns: vec!["username".to_string()],
            },
        ],
        foreign_keys: Vec::new(),
    };

    let error = catalog
        .validate_hot_cold_policy(true, false)
        .expect_err("unique constraints should be rejected when flush is enabled");

    let message = error.to_string();
    assert!(message.contains("users_email_key"));
    assert!(message.contains("email"));
    assert!(message.contains("users_username_idx"));
    assert!(message.contains("username"));
}

#[test]
fn flush_enabled_tables_reject_foreign_keys_with_column_details() {
    let catalog = ManageTableConstraintsCatalog {
        unique_constraints: Vec::new(),
        foreign_keys: vec![
            ForeignKeyShape {
                name: "items_user_id_fkey".to_string(),
                direction: FkDirection::Outbound,
                columns: vec!["user_id".to_string()],
                related_relation: Some("public.users".to_string()),
            },
            ForeignKeyShape {
                name: "orders_item_id_fkey".to_string(),
                direction: FkDirection::Inbound,
                columns: vec!["item_id".to_string()],
                related_relation: Some("public.orders".to_string()),
            },
        ],
    };

    let error = catalog
        .validate_hot_cold_policy(true, false)
        .expect_err("foreign keys should be rejected when flush is enabled");

    let message = error.to_string();
    assert!(message.contains("items_user_id_fkey"));
    assert!(message.contains("user_id"));
    assert!(message.contains("public.users"));
    assert!(message.contains("orders_item_id_fkey"));
    assert!(message.contains("item_id"));
    assert!(message.contains("allow_fk_hot_only"));
}

#[test]
fn hot_only_tables_allow_unique_and_foreign_keys() {
    let catalog = ManageTableConstraintsCatalog {
        unique_constraints: vec![UniqueConstraintShape {
            name: "users_email_key".to_string(),
            columns: vec!["email".to_string()],
        }],
        foreign_keys: vec![ForeignKeyShape {
            name: "items_user_id_fkey".to_string(),
            direction: FkDirection::Outbound,
            columns: vec!["user_id".to_string()],
            related_relation: Some("public.users".to_string()),
        }],
    };

    catalog
        .validate_hot_cold_policy(false, false)
        .expect("hot-only tables should keep native constraint semantics");
}

#[test]
fn migration_validation_records_fk_hot_only_override() {
    let mut input = MigrationValidationInput::minimal_shared();
    input.foreign_keys = vec![ForeignKeyShape {
        name: "items_user_id_fkey".to_string(),
        direction: FkDirection::Outbound,
        columns: vec!["user_id".to_string()],
        related_relation: Some("public.users".to_string()),
    }];
    input.flush_enabled = true;
    input.allow_fk_hot_only = false;
    assert!(input.validate().is_err());

    input.allow_fk_hot_only = true;
    let validation = input.validate().unwrap();
    assert_eq!(
        validation.fk_policy,
        koldstore_migrate::constraints::FkPolicy::AllowHotOnly
    );
}

#[test]
fn migration_validation_rejects_unique_constraints_when_flush_is_enabled() {
    let mut input = MigrationValidationInput::minimal_shared();
    input.flush_enabled = true;
    input.unique_constraints = vec![UniqueConstraintShape {
        name: "users_email_key".to_string(),
        columns: vec!["email".to_string()],
    }];

    let error = input
        .validate()
        .expect_err("unique constraints should fail");
    assert!(error.to_string().contains("users_email_key"));
    assert!(error.to_string().contains("email"));
}
