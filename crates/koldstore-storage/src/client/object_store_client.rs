//! `object_store`-backed client used by flush, manifest, and recovery paths.
//!
//! Durability contract (aligned with `object_store` docs):
//! - Every successful `put` is atomic — readers never observe partial bytes.
//! - Filesystem backends enable `LocalFileSystem::with_fsync(true)` so file
//!   contents and parent directory entries are durable before success returns.
//! - Immutable cold segments use [`PutPrecondition::CreateIfAbsent`].
//! - Manifests may use overwrite, still via atomic staged publish.

use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use crate::object::StorageObject;
use crate::runtime::{self, Elapsed};

use super::api::{PutOutcome, PutPrecondition, StorageClient};
use super::error::{StorageClientError, StorageResult};
use super::keys::{optional_prefix, parse_key};
use super::map_error::{map_object_store_error, map_object_store_error_for_key, map_put_error};

/// `object_store`-backed client used by flush, manifest, and recovery paths.
#[derive(Clone)]
pub struct ObjectStoreClient {
    store: Arc<dyn ObjectStore>,
    /// Absolute filesystem root when using [`LocalFileSystem`]; `None` for memory/S3.
    filesystem_root: Option<std::path::PathBuf>,
    /// Optional outer operation timeout (`None` / zero = disabled).
    timeout: Option<Duration>,
}

impl std::fmt::Debug for ObjectStoreClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreClient")
            .field("filesystem_root", &self.filesystem_root)
            .field("timeout", &self.timeout)
            .field("store", &format_args!("{}", self.store))
            .finish()
    }
}

impl ObjectStoreClient {
    /// Wraps an existing [`ObjectStore`] implementation.
    #[must_use]
    pub fn from_store(
        store: Arc<dyn ObjectStore>,
        filesystem_root: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            store,
            filesystem_root,
            timeout: None,
        }
    }

    /// Sets the outer ObjectStore operation timeout (`None` / zero disables it).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout.filter(|value| !value.is_zero());
        self
    }

    /// Returns the configured outer timeout, if any.
    #[must_use]
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Builds a durable local filesystem client rooted at `root`.
    ///
    /// Enables `with_fsync(true)` so successful puts match remote object-store
    /// durability (file contents + parent directory entries).
    ///
    /// # Errors
    ///
    /// Returns an error when the root cannot be created or opened.
    pub fn local_filesystem(root: impl AsRef<FsPath>) -> StorageResult<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root).map_err(|error| StorageClientError::Backend {
            message: format!("create storage root {}: {error}", root.display()),
        })?;
        let store = LocalFileSystem::new_with_prefix(root)
            .map_err(|error| StorageClientError::Backend {
                message: error.to_string(),
            })?
            .with_fsync(true);
        Ok(Self {
            store: Arc::new(store),
            filesystem_root: Some(root.to_path_buf()),
            timeout: None,
        })
    }

    /// Builds an in-memory client for unit tests.
    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
            filesystem_root: None,
            timeout: None,
        }
    }

    /// Returns true when this client can resolve absolute local filesystem paths.
    #[must_use]
    pub fn is_filesystem(&self) -> bool {
        self.filesystem_root.is_some()
    }

    /// Returns the filesystem root when this client is local-disk backed.
    #[must_use]
    pub fn filesystem_root(&self) -> Option<&FsPath> {
        self.filesystem_root.as_deref()
    }

    /// Resolves an object key to an absolute filesystem path when local.
    ///
    /// # Errors
    ///
    /// Returns an error when the client is not filesystem-backed or the key is
    /// invalid.
    pub fn absolute_path(&self, key: &str) -> StorageResult<std::path::PathBuf> {
        let root =
            self.filesystem_root
                .as_ref()
                .ok_or_else(|| StorageClientError::InvalidPath {
                    message: "absolute_path requires a filesystem-backed client".to_string(),
                })?;
        let location = parse_key(key)?;
        Ok(root.join(location.as_ref()))
    }

    /// Shared store handle for advanced callers (async readers, etc.).
    #[must_use]
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    fn run<T>(
        &self,
        future: impl std::future::Future<Output = StorageResult<T>>,
    ) -> StorageResult<T> {
        match runtime::block_on(future, self.timeout) {
            Ok(result) => result,
            Err(Elapsed) => Err(StorageClientError::Timeout {
                timeout_ms: self
                    .timeout
                    .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
                    .unwrap_or(0),
            }),
        }
    }
}

impl StorageClient for ObjectStoreClient {
    fn list(&self, prefix: &str) -> StorageResult<Vec<StorageObject>> {
        let location = optional_prefix(prefix)?;
        let store = Arc::clone(&self.store);
        self.run(async move {
            let mut stream = store.list(location.as_ref());
            let mut objects = Vec::new();
            while let Some(meta) = stream.next().await {
                let meta = meta.map_err(map_object_store_error)?;
                objects.push(StorageObject {
                    key: meta.location.to_string(),
                    etag: meta.e_tag,
                    byte_size: Some(meta.size),
                });
            }
            Ok(objects)
        })
    }

    fn put(&self, key: &str, bytes: &[u8], mode: PutPrecondition) -> StorageResult<PutOutcome> {
        let location = parse_key(key)?;
        let payload = PutPayload::from(Bytes::copy_from_slice(bytes));
        let opts = PutOptions {
            mode: match mode {
                PutPrecondition::Overwrite => PutMode::Overwrite,
                PutPrecondition::CreateIfAbsent => PutMode::Create,
            },
            ..PutOptions::default()
        };
        let byte_size =
            u64::try_from(bytes.len()).map_err(|error| StorageClientError::Backend {
                message: error.to_string(),
            })?;
        let store = Arc::clone(&self.store);
        let key = key.to_string();
        self.run(async move {
            let result = store
                .put_opts(&location, payload, opts)
                .await
                .map_err(|error| map_put_error(&key, error))?;
            Ok(PutOutcome {
                key,
                etag: result.e_tag,
                byte_size,
            })
        })
    }

    fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
        let location = parse_key(key)?;
        let store = Arc::clone(&self.store);
        let key = key.to_string();
        self.run(async move {
            let result = store
                .get(&location)
                .await
                .map_err(|error| map_object_store_error_for_key(&key, error))?;
            let bytes = result
                .bytes()
                .await
                .map_err(|error| StorageClientError::Backend {
                    message: error.to_string(),
                })?;
            Ok(bytes.to_vec())
        })
    }

    fn head(&self, key: &str) -> StorageResult<StorageObject> {
        let location = parse_key(key)?;
        let store = Arc::clone(&self.store);
        let key = key.to_string();
        self.run(async move {
            let meta = store
                .head(&location)
                .await
                .map_err(|error| map_object_store_error_for_key(&key, error))?;
            Ok(StorageObject {
                key: meta.location.to_string(),
                etag: meta.e_tag,
                byte_size: Some(meta.size),
            })
        })
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        let location = parse_key(key)?;
        let store = Arc::clone(&self.store);
        self.run(async move {
            match store.delete(&location).await {
                Ok(()) => Ok(()),
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(error) => Err(map_object_store_error(error)),
            }
        })
    }

    fn copy_if_absent(&self, from: &str, to: &str) -> StorageResult<()> {
        let from_path = parse_key(from)?;
        let to_path = parse_key(to)?;
        let store = Arc::clone(&self.store);
        let to = to.to_string();
        self.run(async move {
            store
                .copy_if_not_exists(&from_path, &to_path)
                .await
                .map_err(|error| map_put_error(&to, error))
        })
    }
}
