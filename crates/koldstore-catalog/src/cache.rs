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
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let table_oid = value
        .get("table_oid")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "missing integer field `table_oid`".to_string())
        .and_then(|oid| u32::try_from(oid).map_err(|error| error.to_string()))?;
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "missing integer field `schema_version`".to_string())
        .and_then(|version| i32::try_from(version).map_err(|error| error.to_string()))?;
    let active = value
        .get("active")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| "missing boolean field `active`".to_string())?;
    let initialization_state = value
        .get("initialization_state")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| "missing string field `initialization_state`".to_string())?;
    let mirror_relation = value
        .get("mirror_relation")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "missing string field `mirror_relation`".to_string())?;
    let mirror_relation = TableName::parse(mirror_relation).map_err(|error| error.to_string())?;
    let primary_key_columns = serde_json::from_value::<Vec<String>>(
        value
            .get("primary_key")
            .cloned()
            .ok_or_else(|| "missing array field `primary_key`".to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let primary_key_shape = value
        .get("primary_key_shape")
        .ok_or_else(|| "missing field `primary_key_shape`".to_string())?;
    let mut hasher = DefaultHasher::new();
    primary_key_shape.to_string().hash(&mut hasher);
    let scope_column = match value.get("scope_column") {
        Some(scope) if scope.is_null() => None,
        Some(scope) => Some(
            scope
                .as_str()
                .ok_or_else(|| "field `scope_column` must be string or null".to_string())?
                .to_string(),
        ),
        None => None,
    };

    Ok(ManagedTableSnapshot {
        table_oid,
        schema_version,
        active,
        initialization_state,
        mirror_relation,
        primary_key_columns,
        primary_key_shape_hash: hasher.finish(),
        scope_column,
    })
}
