//! Tests for migrated-table schema registry models.

use koldstore_schema::{MirrorInitializationState, TypeMatrix};

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
fn mirror_initialization_state_serializes_as_schema_value() {
    let value = serde_json::to_value(MirrorInitializationState::Capturing).unwrap();

    assert_eq!(value, serde_json::json!("capturing"));
    assert_eq!(MirrorInitializationState::Complete.as_str(), "complete");
    assert_eq!(
        MirrorInitializationState::parse("capturing"),
        Some(MirrorInitializationState::Capturing)
    );
}
