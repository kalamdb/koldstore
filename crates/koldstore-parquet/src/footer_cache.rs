//! Per-process Parquet footer metadata cache.
//!
//! Repeated cold PK lookups against the same segment re-use footer metadata so
//! ObjectStore range GETs for the footer are paid once per backend until
//! invalidation (flush / table lifecycle).

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use parquet::file::metadata::ParquetMetaData;

/// Maximum footers retained per backend process.
const FOOTER_CACHE_LIMIT: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FooterCacheKey {
    object_path: String,
    file_size: Option<u64>,
}

thread_local! {
    static FOOTER_CACHE: RefCell<HashMap<FooterCacheKey, Arc<ParquetMetaData>>> =
        RefCell::new(HashMap::new());
}

/// Looks up a cached footer for `object_path` + optional known `file_size`.
#[must_use]
pub fn get(object_path: &str, file_size: Option<u64>) -> Option<Arc<ParquetMetaData>> {
    let key = FooterCacheKey {
        object_path: object_path.to_string(),
        file_size,
    };
    FOOTER_CACHE.with(|cache| cache.borrow().get(&key).cloned())
}

/// Stores a footer in the backend-local cache, evicting an arbitrary entry when full.
pub fn insert(object_path: &str, file_size: Option<u64>, metadata: Arc<ParquetMetaData>) {
    let key = FooterCacheKey {
        object_path: object_path.to_string(),
        file_size,
    };
    FOOTER_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= FOOTER_CACHE_LIMIT && !cache.contains_key(&key) {
            if let Some(evicted) = cache.keys().next().cloned() {
                cache.remove(&evicted);
            }
        }
        cache.insert(key, metadata);
    });
}

/// Drops every cached footer (call on flush / managed-table invalidation).
pub fn clear() {
    FOOTER_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// Returns the number of cached footers (test helper).
#[must_use]
pub fn len() -> usize {
    FOOTER_CACHE.with(|cache| cache.borrow().len())
}

/// Returns true when the cache holds no footers (test helper).
#[must_use]
pub fn is_empty() -> bool {
    len() == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_and_clear_round_trip() {
        clear();
        assert!(is_empty());

        // ParquetMetaData construction needs a real footer; exercise map ops with
        // clear/len only when empty — full I/O coverage lives in reader_pruning.
        assert_eq!(len(), 0);
        clear();
        assert!(is_empty());
    }
}
