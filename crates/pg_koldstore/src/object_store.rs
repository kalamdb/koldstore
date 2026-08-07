//! Object-store open helpers that apply session GUCs and interrupt hooks.
//!
//! Backend-local client reuse amortizes cloud SDK / HTTP pool construction across
//! cold opens that share the same storage identity. The cache is intentionally
//! tiny (LRU) so long-lived backends cannot accumulate connection pools.

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use koldstore_catalog::OptionalLookupCache;
use koldstore_storage::{
    open_client_from_catalog_fields_with_timeout, ObjectStoreClient, StorageResult,
};

/// Hard cap on cached ObjectStore clients per backend.
///
/// Each cloud client may retain an HTTP connection pool. Keep this small so
/// credential rotation and multi-storage setups stay memory-bounded.
const OBJECT_STORE_CLIENT_CACHE_LIMIT: usize = 8;

/// Identity for one catalog-configured ObjectStore client.
///
/// Credentials and config are fingerprinted (not stored) so the key never retains
/// secrets. Timeout is part of the identity because it is baked into the client.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObjectStoreClientCacheKey {
    storage_type: Arc<str>,
    base_path: Arc<str>,
    config_fingerprint: u64,
    credentials_fingerprint: u64,
    /// Milliseconds; `0` means no outer timeout.
    timeout_ms: u64,
}

thread_local! {
    static CLIENT_CACHE: RefCell<OptionalLookupCache<ObjectStoreClientCacheKey, ObjectStoreClient>> =
        RefCell::new(OptionalLookupCache::with_limit(OBJECT_STORE_CLIENT_CACHE_LIMIT));
}

/// Stable non-cryptographic fingerprint of JSON catalog fields for cache keys.
fn json_fingerprint(value: &serde_json::Value) -> u64 {
    let mut hasher = DefaultHasher::new();
    match serde_json::to_vec(value) {
        Ok(bytes) => bytes.hash(&mut hasher),
        Err(_) => value.to_string().hash(&mut hasher),
    }
    hasher.finish()
}

fn timeout_ms(timeout: Option<Duration>) -> u64 {
    timeout
        .filter(|value| !value.is_zero())
        .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Opens a catalog-configured client with [`crate::guc::object_store_timeout`].
///
/// Repeated opens with the same storage type, base path, config, credentials,
/// and timeout reuse a backend-local cached client (cheap [`Clone`] of an
/// `Arc`-backed store). The cache is cleared on global catalog invalidation.
///
/// # Errors
///
/// Returns storage client construction errors.
pub fn open_managed_object_store_client(
    storage_type: &str,
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
) -> StorageResult<ObjectStoreClient> {
    let timeout = crate::guc::object_store_timeout();
    let key = ObjectStoreClientCacheKey {
        storage_type: Arc::from(storage_type),
        base_path: Arc::from(base_path),
        config_fingerprint: json_fingerprint(config),
        credentials_fingerprint: json_fingerprint(credentials),
        timeout_ms: timeout_ms(timeout),
    };
    if let Some(Some(client)) = CLIENT_CACHE.with(|cache| cache.borrow_mut().get(&key)) {
        return Ok(client);
    }
    let client = open_client_from_catalog_fields_with_timeout(
        storage_type,
        base_path,
        credentials,
        config,
        timeout,
    )?;
    CLIENT_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, Some(client.clone()));
    });
    Ok(client)
}

/// Drops every cached ObjectStore client in this backend.
///
/// Called from global catalog invalidation so credential or location changes
/// cannot leave a live client wired to stale secrets. Per-table invalidation
/// does **not** clear this cache: storage backends are shared across tables and
/// fingerprint misses already replace clients after config changes.
pub fn invalidate_cached_object_store_clients() {
    CLIENT_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// Registers Postgres interrupt checking for ObjectStore waits.
///
/// Call once from `_PG_init` so query cancel drops in-flight HTTP/futures.
#[cfg(feature = "pg")]
pub fn install_interrupt_hook() {
    koldstore_storage::set_interrupt_hook(Some(check_object_store_interrupts));
}

#[cfg(feature = "pg")]
fn check_object_store_interrupts() {
    pgrx::check_for_interrupts!();
}

/// Returns how many ObjectStore clients are cached in this backend (tests).
#[cfg(any(test, feature = "pg_test"))]
#[must_use]
pub fn cached_object_store_client_count() -> usize {
    CLIENT_CACHE.with(|cache| cache.borrow().len())
}

#[cfg(test)]
mod tests {
    use super::{json_fingerprint, timeout_ms, OBJECT_STORE_CLIENT_CACHE_LIMIT};
    use std::time::Duration;

    #[test]
    fn json_fingerprint_is_stable_for_same_object() {
        let left = serde_json::json!({"bucket": "a", "region": "us-east-1"});
        let right = serde_json::json!({"bucket": "a", "region": "us-east-1"});
        assert_eq!(json_fingerprint(&left), json_fingerprint(&right));
    }

    #[test]
    fn json_fingerprint_changes_when_secret_changes() {
        let before = serde_json::json!({"access_key_id": "AKIA", "secret_access_key": "one"});
        let after = serde_json::json!({"access_key_id": "AKIA", "secret_access_key": "two"});
        assert_ne!(json_fingerprint(&before), json_fingerprint(&after));
    }

    #[test]
    fn timeout_ms_treats_none_and_zero_as_disabled() {
        assert_eq!(timeout_ms(None), 0);
        assert_eq!(timeout_ms(Some(Duration::ZERO)), 0);
        assert_eq!(timeout_ms(Some(Duration::from_millis(1500))), 1500);
    }

    #[test]
    fn client_cache_limit_stays_small() {
        assert!(OBJECT_STORE_CLIENT_CACHE_LIMIT <= 16);
        assert!(OBJECT_STORE_CLIENT_CACHE_LIMIT >= 1);
    }
}
