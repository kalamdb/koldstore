//! In-memory model filesystem backend for crash / visibility fault tests.
//!
//! Plan §14.3 foundation: an explicit, snapshotable object-store model that
//! implements [`StorageClient`] without touching the host filesystem.
//!
//! Snapshots are full copies of object bytes. Callers must not retain unbounded
//! snapshot history. Optional [`ModelObjectStore::with_max_object_bytes`] rejects
//! oversized puts in tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use crate::client::{PutOutcome, PutPrecondition, StorageClient};
use crate::object::StorageObject;
use crate::{StorageClientError, StorageResult};

/// One object stored in the model backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelObject {
    /// Object body.
    pub bytes: Vec<u8>,
    /// Optional etag / generation token.
    pub etag: Option<String>,
}

impl ModelObject {
    /// Builds a model object from bytes (etag derived from length).
    #[must_use]
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let etag = Some(format!("etag-{}", bytes.len()));
        Self { bytes, etag }
    }

    /// Byte length of the object body.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the body is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Point-in-time copy of model store state.
///
/// Callers own snapshot lifetime; prefer a small fixed window for crash tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelSnapshot {
    objects: BTreeMap<String, ModelObject>,
    hidden_from_list: BTreeSet<String>,
}

/// In-memory [`StorageClient`] with snapshot / crash-damage helpers.
#[derive(Debug)]
pub struct ModelObjectStore {
    inner: Mutex<ModelState>,
    max_object_bytes: Option<usize>,
}

#[derive(Debug, Default)]
struct ModelState {
    objects: BTreeMap<String, ModelObject>,
    /// Keys present for GET/HEAD but omitted from LIST (visibility fault).
    hidden_from_list: BTreeSet<String>,
}

impl Default for ModelObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelObjectStore {
    /// Empty model store with no object-size cap.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ModelState::default()),
            max_object_bytes: None,
        }
    }

    /// Rejects puts whose body exceeds `max_object_bytes`.
    #[must_use]
    pub fn with_max_object_bytes(mut self, max_object_bytes: usize) -> Self {
        self.max_object_bytes = Some(max_object_bytes);
        self
    }

    /// Copies current object map + visibility faults into a snapshot.
    ///
    /// Snapshots are explicit deep copies. Do not accumulate unbounded history.
    #[must_use]
    pub fn snapshot(&self) -> ModelSnapshot {
        let state = self.inner.lock().expect("model store lock");
        ModelSnapshot {
            objects: state.objects.clone(),
            hidden_from_list: state.hidden_from_list.clone(),
        }
    }

    /// Replaces model state with a previously taken snapshot.
    pub fn restore(&self, snapshot: &ModelSnapshot) {
        let mut state = self.inner.lock().expect("model store lock");
        state.objects = snapshot.objects.clone();
        state.hidden_from_list = snapshot.hidden_from_list.clone();
    }

    /// Removes a key entirely (crash damage: lost object).
    pub fn drop_key(&self, key: &str) {
        let mut state = self.inner.lock().expect("model store lock");
        state.objects.remove(key);
        state.hidden_from_list.remove(key);
    }

    /// Truncates object body to `len` bytes (crash damage: partial write).
    ///
    /// Missing keys are a no-op. Truncation clears the previous etag.
    pub fn truncate_key(&self, key: &str, len: usize) {
        let mut state = self.inner.lock().expect("model store lock");
        if let Some(object) = state.objects.get_mut(key) {
            if object.bytes.len() > len {
                object.bytes.truncate(len);
            }
            object.etag = Some(format!("etag-{}", object.bytes.len()));
        }
    }

    /// Omits `key` from LIST while GET/HEAD still succeed when the object exists.
    pub fn hide_from_list(&self, key: &str) {
        let mut state = self.inner.lock().expect("model store lock");
        state.hidden_from_list.insert(key.to_string());
    }

    /// Clears the hide-from-list fault for `key`.
    pub fn unhide_from_list(&self, key: &str) {
        let mut state = self.inner.lock().expect("model store lock");
        state.hidden_from_list.remove(key);
    }

    /// Number of objects currently stored (including hidden-from-list keys).
    #[must_use]
    pub fn object_count(&self) -> usize {
        self.inner.lock().expect("model store lock").objects.len()
    }
}

impl StorageClient for ModelObjectStore {
    fn list(&self, prefix: &str) -> StorageResult<Vec<StorageObject>> {
        let state = self.inner.lock().expect("model store lock");
        Ok(state
            .objects
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .filter(|(key, _)| !state.hidden_from_list.contains(*key))
            .map(|(key, object)| StorageObject {
                key: key.clone(),
                etag: object.etag.clone(),
                byte_size: Some(object.bytes.len() as u64),
            })
            .collect())
    }

    fn put(&self, key: &str, bytes: &[u8], mode: PutPrecondition) -> StorageResult<PutOutcome> {
        if let Some(max) = self.max_object_bytes {
            if bytes.len() > max {
                return Err(StorageClientError::Backend {
                    message: format!(
                        "model store rejected put: key={key} bytes={} max_object_bytes={max}",
                        bytes.len()
                    ),
                });
            }
        }
        let mut state = self.inner.lock().expect("model store lock");
        if mode == PutPrecondition::CreateIfAbsent && state.objects.contains_key(key) {
            return Err(StorageClientError::AlreadyExists {
                key: key.to_string(),
            });
        }
        let object = ModelObject::from_bytes(bytes.to_vec());
        let etag = object.etag.clone();
        let byte_size = object.bytes.len() as u64;
        state.objects.insert(key.to_string(), object);
        Ok(PutOutcome {
            key: key.to_string(),
            etag,
            byte_size,
        })
    }

    fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
        let state = self.inner.lock().expect("model store lock");
        state
            .objects
            .get(key)
            .map(|object| object.bytes.clone())
            .ok_or_else(|| StorageClientError::NotFound {
                key: key.to_string(),
            })
    }

    fn head(&self, key: &str) -> StorageResult<StorageObject> {
        let state = self.inner.lock().expect("model store lock");
        let object = state
            .objects
            .get(key)
            .ok_or_else(|| StorageClientError::NotFound {
                key: key.to_string(),
            })?;
        Ok(StorageObject {
            key: key.to_string(),
            etag: object.etag.clone(),
            byte_size: Some(object.bytes.len() as u64),
        })
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        let mut state = self.inner.lock().expect("model store lock");
        state.objects.remove(key);
        state.hidden_from_list.remove(key);
        Ok(())
    }

    fn copy_if_absent(&self, from: &str, to: &str) -> StorageResult<()> {
        let mut state = self.inner.lock().expect("model store lock");
        if state.objects.contains_key(to) {
            return Err(StorageClientError::AlreadyExists {
                key: to.to_string(),
            });
        }
        let object =
            state
                .objects
                .get(from)
                .cloned()
                .ok_or_else(|| StorageClientError::NotFound {
                    key: from.to_string(),
                })?;
        state.objects.insert(to.to_string(), object);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ModelObjectStore;
    use crate::client::{PutPrecondition, StorageClient};

    #[test]
    fn put_get_list_round_trip() {
        let store = ModelObjectStore::new();
        store
            .put("a/b", b"hello", PutPrecondition::Overwrite)
            .unwrap();
        assert_eq!(store.get("a/b").unwrap(), b"hello");
        assert_eq!(store.head("a/b").unwrap().byte_size, Some(5));
        assert_eq!(store.list("a/").unwrap().len(), 1);
        assert!(store.list("z/").unwrap().is_empty());
    }

    #[test]
    fn snapshot_restore_replaces_state() {
        let store = ModelObjectStore::new();
        store.put("k", b"v1", PutPrecondition::Overwrite).unwrap();
        let snap = store.snapshot();
        store.put("k", b"v2", PutPrecondition::Overwrite).unwrap();
        store
            .put("other", b"x", PutPrecondition::Overwrite)
            .unwrap();
        assert_eq!(store.object_count(), 2);
        store.restore(&snap);
        assert_eq!(store.get("k").unwrap(), b"v1");
        assert!(store.get("other").is_err());
        assert_eq!(store.object_count(), 1);
    }

    #[test]
    fn truncate_key_damages_object_body() {
        let store = ModelObjectStore::new();
        store
            .put("blob", b"abcdef", PutPrecondition::Overwrite)
            .unwrap();
        store.truncate_key("blob", 3);
        assert_eq!(store.get("blob").unwrap(), b"abc");
        assert_eq!(store.head("blob").unwrap().byte_size, Some(3));
    }

    #[test]
    fn hide_from_list_keeps_get_and_head() {
        let store = ModelObjectStore::new();
        store
            .put("visible", b"1", PutPrecondition::Overwrite)
            .unwrap();
        store
            .put("hidden", b"2", PutPrecondition::Overwrite)
            .unwrap();
        store.hide_from_list("hidden");
        let listed: Vec<_> = store
            .list("")
            .unwrap()
            .into_iter()
            .map(|object| object.key)
            .collect();
        assert_eq!(listed, vec!["visible".to_string()]);
        assert_eq!(store.get("hidden").unwrap(), b"2");
        assert_eq!(store.head("hidden").unwrap().byte_size, Some(1));
        store.unhide_from_list("hidden");
        assert_eq!(store.list("").unwrap().len(), 2);
    }

    #[test]
    fn max_object_bytes_rejects_huge_put() {
        let store = ModelObjectStore::new().with_max_object_bytes(4);
        let err = store
            .put("big", b"12345", PutPrecondition::Overwrite)
            .expect_err("oversize");
        assert!(err.to_string().contains("max_object_bytes"));
        assert!(store.get("big").is_err());
    }

    #[test]
    fn drop_key_removes_object() {
        let store = ModelObjectStore::new();
        store.put("gone", b"x", PutPrecondition::Overwrite).unwrap();
        store.drop_key("gone");
        assert!(store.get("gone").is_err());
    }
}
