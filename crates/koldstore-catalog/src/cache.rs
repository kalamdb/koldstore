//! Managed-table snapshot shapes for catalog caching.
//!
//! Runtime snapshots are assembled from `koldstore.schemas` rows. Schema
//! registry *writes* stay in `koldstore-migrate`; this module owns the
//! PG-free decode + in-process cache shape used by `pg_koldstore`.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use koldstore_common::TableName;
use koldstore_schema::MirrorInitializationState;
use serde::Deserialize;

/// Default cap for optional lookup caches that can grow with query shapes.
pub const DEFAULT_OPTIONAL_LOOKUP_CACHE_LIMIT: usize = 64;

/// Cache that distinguishes an unqueried key from a queried-but-absent value.
///
/// Catalog lookups may legitimately return no row. Keeping that absence avoids
/// repeating the same lookup while still letting invalidation remove the entry.
#[derive(Debug)]
pub struct OptionalLookupCache<K, V> {
    entries: HashMap<K, Option<V>>,
    limit: usize,
}

impl<K, V> Default for OptionalLookupCache<K, V> {
    fn default() -> Self {
        Self::with_limit(DEFAULT_OPTIONAL_LOOKUP_CACHE_LIMIT)
    }
}

impl<K, V> OptionalLookupCache<K, V> {
    /// Builds a cache that evicts an arbitrary entry when `limit` is exceeded.
    #[must_use]
    pub fn with_limit(limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            limit: limit.max(1),
        }
    }
}

impl<K, V> OptionalLookupCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Returns `None` for a cache miss and `Some(None)` for cached absence.
    #[must_use]
    pub fn get(&self, key: &K) -> Option<Option<V>> {
        self.entries.get(key).cloned()
    }

    /// Stores either a present value or a successful absent lookup.
    ///
    /// When the cache is at capacity and `key` is new, one existing entry is
    /// evicted (arbitrary key order, matching the footer-cache policy).
    pub fn insert(&mut self, key: K, value: Option<V>) {
        if self.entries.len() >= self.limit && !self.entries.contains_key(&key) {
            if let Some(evicted) = self.entries.keys().next().cloned() {
                self.entries.remove(&evicted);
            }
        }
        self.entries.insert(key, value);
    }

    /// Retains entries matching `keep`.
    pub fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        self.entries.retain(|key, _| keep(key));
    }

    /// Clears every cached lookup.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Returns the number of cached entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true when the cache holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

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
    pub initialization_state: MirrorInitializationState,
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
///
/// Entries are stored behind [`Arc`] so cache hits can share ownership without
/// cloning the full snapshot on every lookup.
#[derive(Debug, Default)]
pub struct ManagedTableSnapshotCache {
    entries: HashMap<u32, Arc<ManagedTableSnapshot>>,
}

impl ManagedTableSnapshotCache {
    /// Returns a shared snapshot when present.
    #[must_use]
    pub fn get(&self, table_oid: u32) -> Option<Arc<ManagedTableSnapshot>> {
        self.entries.get(&table_oid).cloned()
    }

    /// Stores or replaces a snapshot for a table OID.
    pub fn insert(&mut self, snapshot: ManagedTableSnapshot) {
        self.entries.insert(snapshot.table_oid, Arc::new(snapshot));
    }

    /// Stores an already-shared snapshot.
    pub fn insert_shared(&mut self, snapshot: Arc<ManagedTableSnapshot>) {
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

/// Decodes a stable managed-table snapshot from catalog JSON text.
///
/// Prefer this over [`decode_managed_table_snapshot`] when the SPI payload is
/// already a JSON string — it avoids an intermediate `Value` clone.
///
/// # Errors
///
/// Returns an error when required fields are missing or invalid.
pub fn decode_managed_table_snapshot_str(json: &str) -> Result<ManagedTableSnapshot, String> {
    let wire: ManagedTableSnapshotWire =
        serde_json::from_str(json).map_err(|error| error.to_string())?;
    wire.try_into()
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
        ManagedTableSnapshotWire::deserialize(value).map_err(|error| error.to_string())?;
    wire.try_into()
}

#[derive(Debug, Deserialize)]
struct ManagedTableSnapshotWire {
    table_oid: i64,
    schema_version: i64,
    active: bool,
    initialization_state: MirrorInitializationState,
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

#[cfg(test)]
mod tests {
    use super::OptionalLookupCache;

    #[test]
    fn optional_lookup_cache_distinguishes_miss_from_cached_absence() {
        let mut cache = OptionalLookupCache::<u32, String>::default();

        assert_eq!(cache.get(&42), None);

        cache.insert(42, None);
        assert_eq!(cache.get(&42), Some(None));

        cache.insert(42, Some("manifest".to_string()));
        assert_eq!(cache.get(&42), Some(Some("manifest".to_string())));

        cache.retain(|key| *key != 42);
        assert_eq!(cache.get(&42), None);
    }

    #[test]
    fn optional_lookup_cache_evicts_when_over_limit() {
        let mut cache = OptionalLookupCache::<u32, String>::with_limit(2);
        cache.insert(1, Some("a".to_string()));
        cache.insert(2, Some("b".to_string()));
        cache.insert(3, Some("c".to_string()));
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&3).is_some());
    }
}
