//! Tests for catalog-owned schema versions and column-id allocation.

use koldstore_catalog::{
    active_schema, allocate_column_id, decode_schema_version, schema_at, SchemaColumn,
    SchemaVersion,
};
use koldstore_common::ColumnId;
use serde_json::json;
use uuid::Uuid;

fn version(version: u32, active: bool, next_column_id: u64) -> SchemaVersion {
    SchemaVersion {
        id: Uuid::from_u128(u128::from(version)),
        table_oid: 42,
        version,
        columns: vec![SchemaColumn::app(
            ColumnId::new(1).unwrap(),
            "id",
            "bigint",
            false,
        )],
        next_column_id: ColumnId::new(next_column_id).unwrap(),
        active,
    }
}

#[test]
fn decodes_active_and_selects_historical_schema_versions() {
    let decoded = decode_schema_version(&json!({
        "id": Uuid::from_u128(2),
        "table_oid": 42,
        "version": 2,
        "columns": [{
            "column_id": 1,
            "name": "id",
            "type_name": "bigint",
            "nullable": false,
            "system": false,
            "active": true,
            "attnum": 1,
            "ordinal": 1
        }],
        "next_column_id": 2,
        "active": true
    }))
    .unwrap();
    let versions = vec![version(1, false, 2), decoded.clone()];

    assert_eq!(active_schema(&versions), Some(&decoded));
    assert_eq!(schema_at(&versions, 1), Some(&versions[0]));
    assert_eq!(schema_at(&versions, 2), Some(&decoded));
    assert_eq!(schema_at(&versions, 3), None);
}

#[test]
fn allocation_advances_without_reusing_dropped_column_ids() {
    let next = ColumnId::new(3).unwrap();
    let (allocated, next) = allocate_column_id(next);
    let (allocated_after_gap, new_next) = allocate_column_id(next);

    assert_eq!(allocated.get(), 3);
    assert_eq!(allocated_after_gap.get(), 4);
    assert_eq!(new_next.get(), 5);
}

#[test]
fn schema_column_decode_requires_column_id() {
    let error = decode_schema_version(&json!({
        "id": Uuid::from_u128(1),
        "table_oid": 42,
        "version": 1,
        "columns": [{
            "name": "id",
            "type_name": "bigint",
            "nullable": false
        }],
        "next_column_id": 2,
        "active": true
    }))
    .unwrap_err();

    assert!(error.contains("column_id"));
}

#[test]
fn schema_version_validation_requires_active_primary_key_columns() {
    let schema = version(1, true, 2);

    schema.validate(&["id"]).unwrap();
    assert!(schema.validate(&[]).is_err());
    assert!(schema.validate(&["missing"]).is_err());
    assert_eq!(schema.application_columns().len(), 1);
    assert!(schema.system_columns().is_empty());
}
