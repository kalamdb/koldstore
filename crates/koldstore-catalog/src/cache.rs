//! Managed-table snapshot shapes for catalog caching.
//!
//! Runtime snapshots are assembled from `koldstore.schemas` rows. Schema
//! registry *writes* stay in `koldstore-migrate`; this module owns the
//! PG-free decode + in-process cache shape used by `pg_koldstore`.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use koldstore_common::{ColumnId, ColumnRef, ManageTableOptions, TableName, TableOid};
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
///
/// Eviction is LRU: [`Self::get`] and [`Self::insert`] promote the key to most
/// recently used so hot tables stay resident under the entry cap.
#[derive(Debug)]
pub struct OptionalLookupCache<K, V> {
    entries: HashMap<K, Option<V>>,
    /// Oldest at the front; newest at the back.
    order: std::collections::VecDeque<K>,
    limit: usize,
}

impl<K, V> Default for OptionalLookupCache<K, V> {
    fn default() -> Self {
        Self::with_limit(DEFAULT_OPTIONAL_LOOKUP_CACHE_LIMIT)
    }
}

impl<K, V> OptionalLookupCache<K, V> {
    /// Builds a cache that evicts the least-recently-used entry when `limit` is exceeded.
    #[must_use]
    pub fn with_limit(limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: std::collections::VecDeque::new(),
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
    ///
    /// Hits promote `key` to most-recently-used.
    pub fn get(&mut self, key: &K) -> Option<Option<V>> {
        let value = self.entries.get(key)?.clone();
        self.touch(key);
        Some(value)
    }

    /// Stores either a present value or a successful absent lookup.
    ///
    /// When the cache is at capacity and `key` is new, the least-recently-used
    /// entry is evicted.
    pub fn insert(&mut self, key: K, value: Option<V>) {
        if self.entries.contains_key(&key) {
            self.entries.insert(key.clone(), value);
            self.touch(&key);
            return;
        }
        while self.entries.len() >= self.limit {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            } else {
                break;
            }
        }
        self.entries.insert(key.clone(), value);
        self.order.push_back(key);
    }

    fn touch(&mut self, key: &K) {
        if let Some(index) = self.order.iter().position(|entry| entry == key) {
            if let Some(entry) = self.order.remove(index) {
                self.order.push_back(entry);
            }
        }
    }

    /// Retains entries matching `keep`.
    pub fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        self.entries.retain(|key, _| keep(key));
        self.order.retain(|key| keep(key));
    }

    /// Clears every cached lookup.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
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
/// Evicts the least-recently-used entry when inserting past the configured
/// limit so week-long backends that touch many managed tables stay bounded
/// while keeping hot tables resident.
#[derive(Debug)]
pub struct BoundedOidCache<V> {
    entries: HashMap<u32, V>,
    order: std::collections::VecDeque<u32>,
    limit: usize,
}

impl<V> Default for BoundedOidCache<V> {
    fn default() -> Self {
        Self::with_limit(DEFAULT_OPTIONAL_LOOKUP_CACHE_LIMIT)
    }
}

impl<V> BoundedOidCache<V> {
    /// Builds a cache that evicts the least-recently-used entry when `limit` is exceeded.
    #[must_use]
    pub fn with_limit(limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: std::collections::VecDeque::new(),
            limit: limit.max(1),
        }
    }

    /// Returns a cloned value when present and promotes the key to MRU.
    pub fn get(&mut self, table_oid: u32) -> Option<V>
    where
        V: Clone,
    {
        let value = self.entries.get(&table_oid)?.clone();
        self.touch(table_oid);
        Some(value)
    }

    /// Stores or replaces a value for a table OID.
    pub fn insert(&mut self, table_oid: u32, value: V) {
        use std::collections::hash_map::Entry;

        if let Entry::Occupied(mut occupied) = self.entries.entry(table_oid) {
            occupied.insert(value);
            self.touch(table_oid);
            return;
        }
        while self.entries.len() >= self.limit {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            } else {
                break;
            }
        }
        self.entries.insert(table_oid, value);
        self.order.push_back(table_oid);
    }

    fn touch(&mut self, table_oid: u32) {
        if let Some(index) = self.order.iter().position(|entry| *entry == table_oid) {
            if let Some(entry) = self.order.remove(index) {
                self.order.push_back(entry);
            }
        }
    }

    /// Removes one table from the cache.
    pub fn invalidate(&mut self, table_oid: u32) {
        self.entries.remove(&table_oid);
        self.order.retain(|entry| *entry != table_oid);
    }

    /// Clears all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
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
    pub table_oid: TableOid,
    /// Active schema version.
    pub schema_version: i32,
    /// Whether this schema entry is active.
    pub active: bool,
    /// Mirror initialization state.
    pub initialization_state: MirrorInitializationState,
    /// Active change-log mirror relation.
    pub mirror_relation: TableName,
    /// Preserved primary-key columns with stable identity.
    pub primary_key_columns: Vec<ColumnRef>,
    /// Hash of the exact primary-key shape JSON.
    pub primary_key_shape_hash: u64,
    /// Optional user-scope column.
    pub scope_column: Option<String>,
    /// Stable source attnum used to authorize user-scope segment pruning.
    pub scope_column_id: Option<ColumnId>,
    /// Stable source attnum used for cold-segment ordering and range pruning.
    pub segment_order_column_id: Option<ColumnId>,
}

impl ManagedTableSnapshot {
    /// Returns current-schema primary-key names for SQL and Parquet boundaries.
    pub fn primary_key_names(&self) -> impl Iterator<Item = &str> {
        self.primary_key_columns
            .iter()
            .map(|column| column.name.as_str())
    }
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
    ///
    /// Hits promote the table to most-recently-used.
    pub fn get(&mut self, table_oid: u32) -> Option<Option<Arc<ManagedTableSnapshot>>> {
        self.inner.get(&table_oid)
    }

    /// Stores or replaces a snapshot for a table OID.
    pub fn insert(&mut self, snapshot: ManagedTableSnapshot) {
        let table_oid = snapshot.table_oid.get();
        self.inner.insert(table_oid, Some(Arc::new(snapshot)));
    }

    /// Stores an already-shared snapshot.
    pub fn insert_shared(&mut self, snapshot: Arc<ManagedTableSnapshot>) {
        let table_oid = snapshot.table_oid.get();
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
    primary_key: Vec<ColumnRef>,
    /// Exact PK shape JSON from `koldstore.schemas.primary_key_shape`.
    ///
    /// Kept as [`serde_json::Value`] only for rename-stable hashing: the catalog
    /// blob may be a full [`koldstore_common::PrimaryKeyShape`] object or a
    /// legacy column array, and identity must ignore display `name` fields.
    /// Runtime callers use [`ManagedTableSnapshot::primary_key_columns`], not
    /// this blob. This is not a Sort Key encoding.
    primary_key_shape: serde_json::Value,
    #[serde(default)]
    scope_column: Option<String>,
    /// Typed manage-table options (`koldstore.schemas.options`).
    #[serde(default)]
    options: ManageTableOptions,
}

impl TryFrom<ManagedTableSnapshotWire> for ManagedTableSnapshot {
    type Error = String;

    fn try_from(wire: ManagedTableSnapshotWire) -> Result<Self, Self::Error> {
        use std::collections::hash_map::DefaultHasher;

        let table_oid = u32::try_from(wire.table_oid).map_err(|error| error.to_string())?;
        let table_oid = TableOid::new(table_oid).map_err(|error| error.to_string())?;
        let schema_version =
            i32::try_from(wire.schema_version).map_err(|error| error.to_string())?;
        let mirror_relation =
            TableName::parse(&wire.mirror_relation).map_err(|error| error.to_string())?;
        let mut hasher = DefaultHasher::new();
        for column in &wire.primary_key {
            column.column_id.hash(&mut hasher);
        }
        hash_primary_key_shape(&wire.primary_key_shape, &mut hasher);
        let segment_order_column_id = wire
            .options
            .segment_order_column_id
            .map(ColumnId::from_attnum);
        let scope_column_id = wire.options.scope_column_id.map(ColumnId::from_attnum);

        Ok(Self {
            table_oid,
            schema_version,
            active: wire.active,
            initialization_state: wire.initialization_state,
            mirror_relation,
            primary_key_columns: wire.primary_key,
            primary_key_shape_hash: hasher.finish(),
            scope_column: wire.scope_column.filter(|scope| !scope.is_empty()),
            scope_column_id,
            segment_order_column_id,
        })
    }
}

fn hash_primary_key_shape(value: &serde_json::Value, hasher: &mut impl Hasher) {
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
                hash_primary_key_shape(item, hasher);
            }
        }
        serde_json::Value::Object(map) => {
            5u8.hash(hasher);
            let stable_fields = map
                .iter()
                .filter(|(key, _)| !matches!(key.as_str(), "name" | "column"))
                .collect::<Vec<_>>();
            stable_fields.len().hash(hasher);
            for (key, item) in stable_fields {
                key.hash(hasher);
                hash_primary_key_shape(item, hasher);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use koldstore_common::ColumnId;

    use super::{
        decode_managed_table_snapshot, BoundedOidCache, ManagedTableSnapshotCache,
        OptionalLookupCache,
    };

    #[test]
    fn managed_table_snapshot_decodes_stable_primary_key_refs() {
        let snapshot = decode_managed_table_snapshot(&serde_json::json!({
            "table_oid": 42,
            "schema_version": 3,
            "active": true,
            "initialization_state": "complete",
            "mirror_relation": "koldstore_wal_mirror.items",
            "primary_key": [{"column_id": 7, "name": "renamed_id"}],
            "primary_key_shape": {"columns": [{"column_id": 7, "name": "renamed_id"}]},
            "scope_column": "old_tenant_name",
            "options": {
                "scope_column_id": 4,
                "segment_order_column_id": 9
            }
        }))
        .unwrap();

        assert_eq!(
            snapshot.primary_key_columns[0].column_id,
            ColumnId::from_attnum(7)
        );
        assert_eq!(snapshot.primary_key_columns[0].name, "renamed_id");
        assert_eq!(snapshot.scope_column_id, Some(ColumnId::from_attnum(4)));
        assert_eq!(
            snapshot.segment_order_column_id,
            Some(ColumnId::from_attnum(9))
        );
    }

    #[test]
    fn primary_key_shape_hash_is_stable_across_rename() {
        let snapshot = |name: &str| {
            decode_managed_table_snapshot(&serde_json::json!({
                "table_oid": 42,
                "schema_version": 3,
                "active": true,
                "initialization_state": "complete",
                "mirror_relation": "koldstore_wal_mirror.items",
                "primary_key": [{"column_id": 7, "name": name}],
                "primary_key_shape": {
                    "columns": [{"column_id": 7, "name": name, "type_oid": 20}]
                },
                "scope_column": null
            }))
            .unwrap()
        };

        assert_eq!(
            snapshot("id").primary_key_shape_hash,
            snapshot("renamed_id").primary_key_shape_hash
        );
    }

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
    fn optional_lookup_cache_lru_keeps_recently_used() {
        let mut cache = OptionalLookupCache::<u32, String>::with_limit(2);
        cache.insert(1, Some("a".to_string()));
        cache.insert(2, Some("b".to_string()));
        assert_eq!(cache.get(&1), Some(Some("a".to_string()))); // promote 1
        cache.insert(3, Some("c".to_string())); // evicts LRU=2
        assert_eq!(cache.get(&1), Some(Some("a".to_string())));
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&3), Some(Some("c".to_string())));
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
