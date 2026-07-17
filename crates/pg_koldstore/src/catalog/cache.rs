//! Backend-local managed-table and merge-scan segment caches.

#[cfg(feature = "pg")]
use std::sync::Arc;

#[cfg(feature = "pg")]
use koldstore_catalog::{
    decode::InSyncManifestScanContext, decode_managed_table_snapshot_str, ManagedTableSnapshot,
    ManagedTableSnapshotCache, OptionalLookupCache,
};
#[cfg(feature = "pg")]
use koldstore_merge::scan::plan::SegmentStatsHint;

#[cfg(feature = "pg")]
use crate::spi::{
    execute_prepared, first_row, map_spi_error, require_read_only, select_one, SpiResult,
};

#[cfg(feature = "pg")]
type SegmentStatsCacheKey = (u32, Vec<String>);
#[cfg(feature = "pg")]
type SegmentStatsCache = OptionalLookupCache<SegmentStatsCacheKey, Arc<CachedSegmentStats>>;

#[cfg(feature = "pg")]
thread_local! {
    static MANAGED_TABLE_CACHE: std::cell::RefCell<ManagedTableSnapshotCache> =
        std::cell::RefCell::new(ManagedTableSnapshotCache::default());
    static SEGMENT_STATS_CACHE: std::cell::RefCell<SegmentStatsCache> =
        std::cell::RefCell::new(OptionalLookupCache::default());
    static MIGRATION_CATALOG_CACHE: std::cell::RefCell<
        std::collections::HashMap<u32, Arc<koldstore_migrate::ExistingTableCatalog>>
    > = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Cached cold-segment metadata for one managed table.
#[cfg(feature = "pg")]
#[derive(Debug, Clone)]
pub struct CachedSegmentStats {
    /// Published manifest object path.
    pub manifest_path: String,
    /// Manifest generation used as the cache identity.
    pub generation: u64,
    /// Object-store base path.
    pub base_path: String,
    /// Catalog storage backend type.
    pub storage_type: String,
    /// Storage credentials JSON.
    pub credentials: serde_json::Value,
    /// Storage backend config JSON.
    pub config: serde_json::Value,
    /// Active shared-scope segment stats.
    pub segments: Vec<SegmentStatsHint>,
}

/// Registers the relcache callback that keeps backend-local KoldStore caches coherent.
#[cfg(feature = "pg")]
pub fn register_invalidation_callback() {
    unsafe {
        pgrx::pg_sys::CacheRegisterRelcacheCallback(
            Some(relcache_invalidation_callback),
            pgrx::pg_sys::Datum::from(0usize),
        );
    }
}

/// Invalidates one table locally and broadcasts a relcache invalidation to other backends.
///
/// Flush completion uses this after publishing a new manifest so backends that
/// cached the pre-flush absence reload cold-segment metadata before their next
/// managed-table plan or execution.
#[cfg(feature = "pg")]
pub fn invalidate_table_globally(table_oid: pgrx::pg_sys::Oid) {
    invalidate_table(table_oid);
    unsafe {
        pgrx::pg_sys::CacheInvalidateRelcacheByRelid(table_oid);
    }
}

#[cfg(feature = "pg")]
#[pgrx::pg_guard]
unsafe extern "C-unwind" fn relcache_invalidation_callback(
    _arg: pgrx::pg_sys::Datum,
    table_oid: pgrx::pg_sys::Oid,
) {
    if table_oid == pgrx::pg_sys::InvalidOid {
        invalidate_all();
    } else {
        invalidate_table(table_oid);
    }
}

/// Invalidates one managed-table snapshot and segment-stats entry in this backend.
#[cfg(feature = "pg")]
pub fn invalidate_table(table_oid: pgrx::pg_sys::Oid) {
    let key = table_oid.to_u32();
    MANAGED_TABLE_CACHE.with(|cache| {
        cache.borrow_mut().invalidate(key);
    });
    SEGMENT_STATS_CACHE.with(|cache| {
        cache
            .borrow_mut()
            .retain(|(table_oid, _)| *table_oid != key);
    });
    MIGRATION_CATALOG_CACHE.with(|cache| {
        cache.borrow_mut().remove(&key);
    });
    // Footers are path-keyed across tables; drop them on any managed-table change.
    koldstore_parquet::parquet_footer_cache::clear();
}

/// Invalidates all managed-table snapshots and segment-stats entries in this backend.
#[cfg(feature = "pg")]
pub fn invalidate_all() {
    MANAGED_TABLE_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
    SEGMENT_STATS_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
    MIGRATION_CATALOG_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
    koldstore_parquet::parquet_footer_cache::clear();
}

/// Loads the migration catalog (columns / PK / indexed) from cache or SPI.
///
/// Merge scan calls this on every `BeginCustomScan`; caching avoids three
/// introspection SPI round-trips per point lookup.
///
/// # Errors
///
/// Returns an error when SPI introspection or catalog decoding fails.
#[cfg(feature = "pg")]
pub fn cached_migration_catalog(
    table_oid: pgrx::pg_sys::Oid,
) -> Result<Arc<koldstore_migrate::ExistingTableCatalog>, String> {
    let key = table_oid.to_u32();
    if let Some(cached) = MIGRATION_CATALOG_CACHE.with(|cache| cache.borrow().get(&key).cloned()) {
        return Ok(cached);
    }
    let catalog = crate::sql::migrate_pg::load_migration_catalog(key)?;
    let shared = Arc::new(catalog);
    MIGRATION_CATALOG_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, Arc::clone(&shared));
    });
    Ok(shared)
}

/// Loads a managed-table snapshot from cache or catalog.
///
/// Cache hits share an [`Arc`] so callers avoid cloning the full snapshot.
///
/// # Errors
///
/// Returns an error when SPI execution or snapshot decoding fails.
#[cfg(feature = "pg")]
pub fn managed_table_snapshot(
    table_oid: pgrx::pg_sys::Oid,
) -> SpiResult<Option<Arc<ManagedTableSnapshot>>> {
    let key = table_oid.to_u32();
    if let Some(snapshot) = MANAGED_TABLE_CACHE.with(|cache| cache.borrow().get(key)) {
        return Ok(Some(snapshot));
    }

    let snapshot = load_managed_table_snapshot(table_oid)?;
    if let Some(snapshot) = snapshot.as_ref() {
        MANAGED_TABLE_CACHE.with(|cache| {
            cache.borrow_mut().insert_shared(Arc::clone(snapshot));
        });
    }
    Ok(snapshot)
}

/// Loads published manifest path, base path, and active segment stats for merge scan.
///
/// Returns `Ok(None)` when no published manifest exists (hot-only / pre-flush).
/// Hot DML that dirties `sync_state` to `pending_write` still returns the last
/// published cold segments.
/// Both present and absent lookups are cached. Flush completion broadcasts a
/// relcache invalidation for the managed table before later scans can reuse the
/// entry.
///
/// # Errors
///
/// Returns an error when SPI execution or JSON decoding fails.
#[cfg(feature = "pg")]
pub fn cached_manifest_segment_stats(
    table_oid: pgrx::pg_sys::Oid,
    predicate_columns: &[String],
) -> Result<Option<Arc<CachedSegmentStats>>, String> {
    let key = table_oid.to_u32();
    let mut columns = predicate_columns.to_vec();
    columns.sort();
    columns.dedup();
    let cache_key = (key, columns);
    if let Some(cached) = SEGMENT_STATS_CACHE.with(|cache| cache.borrow().get(&cache_key)) {
        return Ok(cached);
    }

    let shared = load_manifest_segment_stats(table_oid, &cache_key.1)?.map(Arc::new);
    SEGMENT_STATS_CACHE.with(|cache| {
        cache.borrow_mut().insert(cache_key, shared.clone());
    });
    Ok(shared)
}

#[cfg(feature = "pg")]
fn load_managed_table_snapshot(
    table_oid: pgrx::pg_sys::Oid,
) -> SpiResult<Option<Arc<ManagedTableSnapshot>>> {
    let statement = koldstore_catalog::queries::plan_managed_table_snapshot()?;
    let json = select_one::<String>(&statement, &[pgrx::datum::DatumWithOid::from(table_oid)])?;
    json.map(|json| {
        decode_managed_table_snapshot_str(&json)
            .map(Arc::new)
            .map_err(|error| map_spi_error(&statement.operation, &error))
    })
    .transpose()
}

#[cfg(feature = "pg")]
fn load_manifest_segment_stats(
    table_oid: pgrx::pg_sys::Oid,
    predicate_columns: &[String],
) -> Result<Option<CachedSegmentStats>, String> {
    let statement = koldstore_catalog::queries::plan_in_sync_manifest_scan_context()
        .map_err(|error| error.to_string())?;
    require_read_only(&statement).map_err(|error| error.to_string())?;
    let json = execute_prepared(
        &statement,
        &[
            pgrx::datum::DatumWithOid::from(table_oid),
            pgrx::datum::DatumWithOid::from(pgrx::JsonB(serde_json::json!(predicate_columns))),
        ],
        first_row::<String>,
    )
    .map_err(|error| error.to_string())?;
    let Some(json) = json else {
        return Ok(None);
    };
    let value: serde_json::Value =
        serde_json::from_str(&json).map_err(|error| error.to_string())?;
    let context = koldstore_catalog::decode::in_sync_manifest_scan_context(&value)?;
    Ok(Some(cached_from_context(context)))
}

#[cfg(feature = "pg")]
fn cached_from_context(context: InSyncManifestScanContext) -> CachedSegmentStats {
    CachedSegmentStats {
        manifest_path: context.manifest_path,
        generation: context.generation,
        base_path: context.base_path,
        storage_type: context.storage_type,
        credentials: context.credentials,
        config: context.config,
        segments: context
            .segments
            .into_iter()
            .map(|segment| SegmentStatsHint {
                object_path: segment.object_path,
                column_stats: catalog_column_stats_map(segment.column_stats),
                byte_size: segment.byte_size,
            })
            .collect(),
    }
}

#[cfg(feature = "pg")]
fn catalog_column_stats_map(
    column_stats: serde_json::Value,
) -> std::collections::BTreeMap<String, koldstore_parquet::ColumnStats> {
    koldstore_catalog::column_stats_min_max_map(&column_stats)
        .into_iter()
        .map(|(column, (min, max))| (column, koldstore_parquet::ColumnStats { min, max }))
        .collect()
}
