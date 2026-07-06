//! Tests for migrated-table schema registry models.

use koldstore_schema::{MirrorInitializationState, SchemaColumn, SchemaRegistryEntry, TypeMatrix};
use uuid::Uuid;

#[test]
fn type_matrix_reports_supported_and_unsupported_types() {
    let matrix = TypeMatrix::postgres_15_default();

    assert!(matrix.support_for("int8").supported);
    assert!(matrix.support_for("text").supported);
    let unsupported = matrix.support_for("tsvector");
    assert!(!unsupported.supported);
    assert!(unsupported
        .diagnostic
        .unwrap()
        .contains("unsupported PostgreSQL type"));
}

#[test]
fn schema_registry_validation_requires_pk_but_not_system_columns() {
    let entry = SchemaRegistryEntry {
        id: Uuid::new_v4(),
        table_oid: 42,
        version: 1,
        columns: vec![SchemaColumn::app("id", "int8", false)],
    };

    entry.validate(&["id"]).unwrap();
    assert!(entry.validate(&[]).is_err());
    assert!(entry.validate(&["missing"]).is_err());
    assert_eq!(entry.application_columns().len(), 1);
    assert!(entry.system_columns().is_empty());
}

#[test]
fn mirror_initialization_state_serializes_as_schema_value() {
    let value = serde_json::to_value(MirrorInitializationState::Capturing).unwrap();

    assert_eq!(value, serde_json::json!("capturing"));
}
