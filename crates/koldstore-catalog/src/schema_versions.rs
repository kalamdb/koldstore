//! Versioned schema access owned by the catalog crate.
//!
//! SQL plans for loading these values live in [`crate::queries`]; SPI execution
//! remains in the extension crate.

use serde_json::Value;

use koldstore_common::ColumnId;

use crate::SchemaVersion;

/// Decodes one complete schema-version JSON row.
///
/// # Errors
///
/// Returns an error when required fields are absent or a column type is invalid.
pub fn decode_schema_version(value: &Value) -> Result<SchemaVersion, String> {
    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
}

/// Returns the active schema with the highest version number.
#[must_use]
pub fn active_schema(versions: &[SchemaVersion]) -> Option<&SchemaVersion> {
    versions
        .iter()
        .filter(|schema| schema.active)
        .max_by_key(|schema| schema.version)
}

/// Returns a specific historical schema version.
#[must_use]
pub fn schema_at(versions: &[SchemaVersion], version: u32) -> Option<&SchemaVersion> {
    versions.iter().find(|schema| schema.version == version)
}

/// Allocates `next` and advances the durable high-water mark.
///
/// Column identifiers are never derived from the currently active columns, so
/// dropping a column cannot make its identifier reusable.
///
/// # Panics
///
/// Panics only when the `u64` column-id space is exhausted.
#[must_use]
pub fn allocate_column_id(next: ColumnId) -> (ColumnId, ColumnId) {
    let new_next = next
        .get()
        .checked_add(1)
        .and_then(|value| ColumnId::new(value).ok())
        .expect("column id space exhausted");
    (next, new_next)
}
