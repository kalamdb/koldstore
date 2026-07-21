//! Managed-table snapshot shapes for catalog caching.
//!
//! Runtime snapshots are assembled from `koldstore.schemas` rows. Schema
//! registry *writes* stay in `koldstore-migrate`; this module owns the
//! PG-free decode + in-process cache shape used by `pg_koldstore`.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use koldstore_common::TableName;
use koldstore_schema::MirrorInitializationState;
use serde::Deserialize;

/// Default cap for OID-keyed and optional lookup caches.
pub const DEFAULT_OPTIONAL_LOOKUP_CACHE_LIMIT: usize = 64;

/// Cap for managed-table snapshot lookups (present + absent).
///
/// Planner hooks consult this on every base relation of a `SELECT`. Unmanaged
/// tables must keep a cached `None` so the hot path stays in-memory; a larger
/// budget avoids thrashing on databases with many ordinary heaps.
pub const MANAGED_TABLE_SNAPSHOT_CACHE_LIMIT: usize = 1024;

const _: () = assert!(MANAGED_TABLE_SNAPSHOT_CACHE_LIMIT >= 1024);

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

/// Bounded in-process map keyed by table OID.
///
/// Evicts an arbitrary entry when inserting past [`DEFAULT_OPTIONAL_LOOKUP_CACHE_LIMIT`]
/// so week-long backends that touch many managed tables do not grow without bound.
#[derive(Debug)]
pub struct BoundedOidCache<V> {
    entries: HashMap<u32, V>,
    limit: usize,
}

impl<V> Default for BoundedOidCache<V> {
    fn default() -> Self {
        Self::with_limit(DEFAULT_OPTIONAL_LOOKUP_CACHE_LIMIT)
    }
}

impl<V> BoundedOidCache<V> {
    /// Builds a cache that evicts an arbitrary entry when `limit` is exceeded.
    #[must_use]
    pub fn with_limit(limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            limit: limit.max(1),
        }
    }

    /// Returns a shared reference when present.
    #[must_use]
    pub fn get(&self, table_oid: u32) -> Option<&V> {
        self.entries.get(&table_oid)
    }

    /// Stores or replaces a value for a table OID.
    pub fn insert(&mut self, table_oid: u32, value: V) {
        if self.entries.len() >= self.limit && !self.entries.contains_key(&table_oid) {
            if let Some(evicted) = self.entries.keys().next().copied() {
                self.entries.remove(&evicted);
            }
        }
        self.entries.insert(table_oid, value);
    }

    /// Removes one table from the cache.
    pub fn invalidate(&mut self, table_oid: u32) {
        self.entries.remove(&table_oid);
    }

    /// Clears all entries.
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
/// Both present snapshots and successful absences (`None`) are cached so the
/// planner hook does not SPI-query `koldstore.schemas` on every unmanaged
/// `SELECT`. Entries are stored behind [`Arc`] so hits share ownership without
/// cloning. Capacity is capped at [`MANAGED_TABLE_SNAPSHOT_CACHE_LIMIT`].
#[derive(Debug)]
pub struct ManagedTableSnapshotCache {
    inner: OptionalLookupCache<u32, Arc<ManagedTableSnapshot>>,
}

impl Default for ManagedTableSnapshotCache {
    fn default() -> Self {
        Self::with_limit(MANAGED_TABLE_SNAPSHOT_CACHE_LIMIT)
    }
}

impl ManagedTableSnapshotCache {
    /// Builds a cache with an explicit entry limit.
    #[must_use]
    pub fn with_limit(limit: usize) -> Self {
        Self {
            inner: OptionalLookupCache::with_limit(limit),
        }
    }

    /// Returns `None` on cache miss, `Some(None)` for cached absence, and
    /// `Some(Some(snapshot))` for a cached managed-table snapshot.
    #[must_use]
    pub fn get(&self, table_oid: u32) -> Option<Option<Arc<ManagedTableSnapshot>>> {
        self.inner.get(&table_oid)
    }

    /// Stores or replaces a snapshot for a table OID.
    pub fn insert(&mut self, snapshot: ManagedTableSnapshot) {
        let table_oid = snapshot.table_oid;
        self.inner.insert(table_oid, Some(Arc::new(snapshot)));
    }

    /// Stores an already-shared snapshot.
    pub fn insert_shared(&mut self, snapshot: Arc<ManagedTableSnapshot>) {
        let table_oid = snapshot.table_oid;
        self.inner.insert(table_oid, Some(snapshot));
    }

    /// Caches a successful lookup that found no managed-table row.
    pub fn insert_absent(&mut self, table_oid: u32) {
        self.inner.insert(table_oid, None);
    }

    /// Removes one table from the cache (present or absent entry).
    pub fn invalidate(&mut self, table_oid: u32) {
        self.inner.retain(|oid| *oid != table_oid);
    }

    /// Clears all cached snapshots and absences.
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Returns the number of cached entries (present + absent).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true when the cache holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
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
        for column in &wire.primary_key {
            column.hash(&mut hasher);
        }
        hash_json_value(&wire.primary_key_shape, &mut hasher);

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

fn hash_json_value(value: &serde_json::Value, hasher: &mut impl Hasher) {
    match value {
        serde_json::Value::Null => 0u8.hash(hasher),
        serde_json::Value::Bool(flag) => {
            1u8.hash(hasher);
            flag.hash(hasher);
        }
        serde_json::Value::Number(number) => {
            2u8.hash(hasher);
            number.as_i64().hash(hasher);
            number.as_u64().hash(hasher);
            number.as_f64().map(f64::to_bits).hash(hasher);
        }
        serde_json::Value::String(text) => {
            3u8.hash(hasher);
            text.hash(hasher);
        }
        serde_json::Value::Array(items) => {
            4u8.hash(hasher);
            items.len().hash(hasher);
            for item in items {
                hash_json_value(item, hasher);
            }
        }
        serde_json::Value::Object(map) => {
            5u8.hash(hasher);
            map.len().hash(hasher);
            for (key, item) in map {
                key.hash(hasher);
                hash_json_value(item, hasher);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedOidCache, ManagedTableSnapshotCache, OptionalLookupCache};

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

    #[test]
    fn bounded_oid_cache_evicts_when_over_limit() {
        let mut cache = BoundedOidCache::with_limit(2);
        cache.insert(1, "a".to_string());
        cache.insert(2, "b".to_string());
        cache.insert(3, "c".to_string());
        assert_eq!(cache.len(), 2);
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn managed_table_snapshot_cache_stores_absence() {
        let mut cache = ManagedTableSnapshotCache::default();
        assert_eq!(cache.get(99), None);
        cache.insert_absent(99);
        assert_eq!(cache.get(99), Some(None));
        assert_eq!(cache.len(), 1);
        cache.invalidate(99);
        assert_eq!(cache.get(99), None);
    }
}
