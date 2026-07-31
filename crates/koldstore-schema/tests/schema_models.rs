//! Tests for migrated-table schema registry models.

use koldstore_common::ColumnId;
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
fn schema_column_serialization_includes_column_id() {
    let column = SchemaColumn::app(3, "created_at", "timestamptz", false);
    let value = serde_json::to_value(&column).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "column_id": 3,
            "name": "created_at",
            "type_name": "timestamptz",
            "nullable": false
        })
    );

    let decoded: SchemaColumn = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.column_id, ColumnId::from_attnum(3));
    assert_eq!(decoded.name, "created_at");
}

#[test]
fn schema_registry_validation_requires_pk_ids() {
    let entry = SchemaRegistryEntry {
        id: Uuid::new_v4(),
        table_oid: 42,
        version: 1,
        columns: vec![SchemaColumn::app(1, "id", "int8", false)],
    };

    entry.validate(&[ColumnId::from_attnum(1)]).unwrap();
    assert!(entry.validate(&[]).is_err());
    assert!(entry.validate(&[ColumnId::from_attnum(99)]).is_err());
    assert_eq!(entry.application_columns().len(), 1);
    assert_eq!(entry.physical_name(ColumnId::from_attnum(1)), Some("id"));
}

#[test]
fn rename_preserves_column_id_in_new_version() {
    let v1 = SchemaColumn::app(3, "created_at", "timestamptz", false);
    let v2 = SchemaColumn::app(3, "event_time", "timestamptz", false);
    assert_eq!(v1.column_id, v2.column_id);
    assert_ne!(v1.name, v2.name);
}

#[test]
fn mirror_initialization_state_serializes_as_schema_value() {
    let value = serde_json::to_value(MirrorInitializationState::Capturing).unwrap();

    assert_eq!(value, serde_json::json!("capturing"));
    assert_eq!(MirrorInitializationState::Complete.as_str(), "complete");
    assert_eq!(
        MirrorInitializationState::parse("capturing"),
        Some(MirrorInitializationState::Capturing)
    );
}
