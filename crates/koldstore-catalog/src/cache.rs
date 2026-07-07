//! Managed-table snapshot shapes for catalog caching.

use koldstore_common::TableName;

/// Stable table-shape metadata for one managed table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedTableSnapshot {
    /// Source table OID.
    pub table_oid: u32,
    /// Active schema version.
    pub schema_version: i32,
    /// Whether this schema entry is active.
    pub active: bool,
    /// Mirror initialization state.
    pub initialization_state: String,
    /// Active change-log mirror relation.
    pub mirror_relation: TableName,
    /// Preserved primary-key columns.
    pub primary_key_columns: Vec<String>,
    /// Hash of the exact primary-key shape JSON.
    pub primary_key_shape_hash: u64,
    /// Optional user-scope column.
    pub scope_column: Option<String>,
}

/// In-process cache keyed by table OID.
#[derive(Debug, Default)]
pub struct ManagedTableSnapshotCache {
    entries: std::collections::HashMap<u32, ManagedTableSnapshot>,
}

impl ManagedTableSnapshotCache {
    /// Returns a cached snapshot when present.
    #[must_use]
    pub fn get(&self, table_oid: u32) -> Option<&ManagedTableSnapshot> {
        self.entries.get(&table_oid)
    }

    /// Stores or replaces a snapshot for a table OID.
    pub fn insert(&mut self, snapshot: ManagedTableSnapshot) {
        self.entries.insert(snapshot.table_oid, snapshot);
    }

    /// Removes one table from the cache.
    pub fn invalidate(&mut self, table_oid: u32) {
        self.entries.remove(&table_oid);
    }

    /// Clears all cached snapshots.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Decodes a stable managed-table snapshot from catalog JSON.
///
/// # Errors
///
/// Returns an error when required fields are missing or invalid.
pub fn decode_managed_table_snapshot(
    value: &serde_json::Value,
) -> Result<ManagedTableSnapshot, String> {
    let wire: ManagedTableSnapshotWire =
        serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
    wire.try_into()
}

#[derive(Debug, serde::Deserialize)]
struct ManagedTableSnapshotWire {
    table_oid: i64,
    schema_version: i64,
    active: bool,
    initialization_state: String,
    mirror_relation: String,
    primary_key: Vec<String>,
    primary_key_shape: serde_json::Value,
    #[serde(default)]
    scope_column: Option<serde_json::Value>,
}

impl TryFrom<ManagedTableSnapshotWire> for ManagedTableSnapshot {
    type Error = String;

    fn try_from(wire: ManagedTableSnapshotWire) -> Result<Self, Self::Error> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let table_oid = u32::try_from(wire.table_oid).map_err(|error| error.to_string())?;
        let schema_version =
            i32::try_from(wire.schema_version).map_err(|error| error.to_string())?;
        let mirror_relation =
            TableName::parse(&wire.mirror_relation).map_err(|error| error.to_string())?;
        let scope_column = match wire.scope_column {
            None => None,
            Some(scope) if scope.is_null() => None,
            Some(scope) => Some(
                scope
                    .as_str()
                    .ok_or_else(|| "field `scope_column` must be string or null".to_string())?
                    .to_string(),
            ),
        };
        let mut hasher = DefaultHasher::new();
        wire.primary_key_shape.to_string().hash(&mut hasher);

        Ok(Self {
            table_oid,
            schema_version,
            active: wire.active,
            initialization_state: wire.initialization_state,
            mirror_relation,
            primary_key_columns: wire.primary_key,
            primary_key_shape_hash: hasher.finish(),
            scope_column,
        })
    }
}
