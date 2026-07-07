#[test]
fn migration_sql_enforces_fk_hot_only_policy() {
    assert!(koldstore_migrate::constraints::fk_policy_allowed(
        true, true, true
    ));
    assert!(koldstore_migrate::constraints::fk_policy_allowed(
        true, false, false
    ));
    assert!(!koldstore_migrate::constraints::fk_policy_allowed(
        true, true, false
    ));
}

#[test]
fn migration_validation_records_fk_hot_only_override() {
    use koldstore_migrate::constraints::{
        FkDirection, FkPolicy, ForeignKeyShape, MigrationValidationInput,
    };

    let mut input = MigrationValidationInput::minimal_shared();
    input.foreign_keys = vec![ForeignKeyShape {
        name: "items_user_id_fkey".to_string(),
        direction: FkDirection::Outbound,
    }];
    input.flush_enabled = true;
    input.allow_fk_hot_only = false;
    assert!(input.validate().is_err());

    input.allow_fk_hot_only = true;
    let validation = input.validate().unwrap();
    assert_eq!(validation.fk_policy, FkPolicy::AllowHotOnly);

    input.allow_fk_hot_only = false;
    input.flush_enabled = false;
    let validation = input.validate().unwrap();
    assert_eq!(validation.fk_policy, FkPolicy::Native);
}

#[test]
fn migration_validation_rejects_inbound_fk_with_flush_without_override() {
    use koldstore_migrate::constraints::{FkDirection, ForeignKeyShape, MigrationValidationInput};

    let mut input = MigrationValidationInput::minimal_shared();
    input.foreign_keys = vec![ForeignKeyShape {
        name: "orders_item_id_fkey".to_string(),
        direction: FkDirection::Inbound,
    }];
    input.flush_enabled = true;

    assert!(input.validate().is_err());
}
