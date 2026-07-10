//! Object storage client trait and `object_store`-backed implementation.
//!
//! Durability contract (aligned with `object_store` docs):
//! - Every successful `put` is atomic — readers never observe partial bytes.
//! - Filesystem backends enable `LocalFileSystem::with_fsync(true)` so file
//!   contents and parent directory entries are durable before success returns.
//! - Immutable cold segments use [`PutPrecondition::CreateIfAbsent`].
//! - Manifests may use overwrite, still via atomic staged publish.

use std::future::Future;
use std::path::Path as FsPath;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use futures_util::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use thiserror::Error;

use crate::object::StorageObject;

/// Storage client operation result.
pub type StorageResult<T> = Result<T, StorageClientError>;

/// Error returned by storage client implementations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StorageClientError {
    /// Object was not found.
    #[error("object not found: {key}")]
    NotFound { key: String },
    /// Conditional create failed because the object already exists.
    #[error("object already exists: {key}")]
    AlreadyExists { key: String },
    /// Object exists but failed validation (size/content).
    #[error("object validation failed for {key}: {message}")]
    Validation { key: String, message: String },
    /// Path or configuration is invalid.
    #[error("invalid storage path: {message}")]
    InvalidPath { message: String },
    /// Backend rejected the request.
    #[error("storage backend error: {message}")]
    Backend { message: String },
}

/// Write precondition for durable puts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutPrecondition {
    /// Replace any existing object via atomic staged publish.
    Overwrite,
    /// Succeed only when the target key is absent (`PutMode::Create`).
    CreateIfAbsent,
}

/// Result of a successful put.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOutcome {
    /// Object key that was written.
    pub key: String,
    /// Optional content etag from the backend.
    pub etag: Option<String>,
    /// Number of bytes written.
    pub byte_size: u64,
}

/// Backend-agnostic object storage access.
pub trait StorageClient {
    /// Lists objects under a prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend request fails.
    fn list(&self, prefix: &str) -> StorageResult<Vec<StorageObject>>;

    /// Uploads object bytes with the given precondition.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend request fails or CreateIfAbsent races.
    fn put(&self, key: &str, bytes: &[u8], mode: PutPrecondition) -> StorageResult<PutOutcome>;

    /// Downloads object bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the object is missing or the backend request fails.
    fn get(&self, key: &str) -> StorageResult<Vec<u8>>;

    /// Returns object metadata without downloading the body.
    ///
    /// # Errors
    ///
    /// Returns an error when the object is missing or the backend request fails.
    fn head(&self, key: &str) -> StorageResult<StorageObject>;

    /// Deletes one object.
    ///
    /// Missing keys are treated as success (idempotent delete).
    ///
    /// # Errors
    ///
    /// Returns an error when the backend request fails for a reason other than
    /// not-found.
    fn delete(&self, key: &str) -> StorageResult<()>;

    /// Copies `from` to `to` only when `to` is absent.
    ///
    /// # Errors
    ///
    /// Returns [`StorageClientError::AlreadyExists`] when `to` exists, or a
    /// backend error on failure.
    fn copy_if_absent(&self, from: &str, to: &str) -> StorageResult<()>;
}

/// `object_store`-backed client used by flush, manifest, and recovery paths.
#[derive(Clone)]
pub struct ObjectStoreClient {
    store: Arc<dyn ObjectStore>,
    /// Absolute filesystem root when using [`LocalFileSystem`]; `None` for memory/S3.
    filesystem_root: Option<std::path::PathBuf>,
}

impl std::fmt::Debug for ObjectStoreClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreClient")
            .field("filesystem_root", &self.filesystem_root)
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
        }
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
        })
    }

    /// Builds an in-memory client for unit tests.
    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
            filesystem_root: None,
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
}

impl StorageClient for ObjectStoreClient {
    fn list(&self, prefix: &str) -> StorageResult<Vec<StorageObject>> {
        let location = optional_prefix(prefix)?;
        block_on(async {
            let mut stream = self.store.list(location.as_ref());
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
        block_on(async {
            let result = self
                .store
                .put_opts(&location, payload, opts)
                .await
                .map_err(|error| map_put_error(key, error))?;
            Ok(PutOutcome {
                key: key.to_string(),
                etag: result.e_tag,
                byte_size,
            })
        })
    }

    fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
        let location = parse_key(key)?;
        block_on(async {
            let result = self
                .store
                .get(&location)
                .await
                .map_err(|error| map_object_store_error_for_key(key, error))?;
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
        block_on(async {
            let meta = self
                .store
                .head(&location)
                .await
                .map_err(|error| map_object_store_error_for_key(key, error))?;
            Ok(StorageObject {
                key: meta.location.to_string(),
                etag: meta.e_tag,
                byte_size: Some(meta.size),
            })
        })
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        let location = parse_key(key)?;
        block_on(async {
            match self.store.delete(&location).await {
                Ok(()) => Ok(()),
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(error) => Err(map_object_store_error(error)),
            }
        })
    }

    fn copy_if_absent(&self, from: &str, to: &str) -> StorageResult<()> {
        let from_path = parse_key(from)?;
        let to_path = parse_key(to)?;
        block_on(async {
            self.store
                .copy_if_not_exists(&from_path, &to_path)
                .await
                .map_err(|error| map_put_error(to, error))
        })
    }
}

fn parse_key(key: &str) -> StorageResult<Path> {
    let trimmed = key.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: "object key must not be empty".to_string(),
        });
    }
    if trimmed.split('/').any(|part| part == "." || part == "..") {
        return Err(StorageClientError::InvalidPath {
            message: format!("object key must not contain '.' or '..' segments: {key}"),
        });
    }
    // object_store LocalFileSystem reserves trailing `/#\d+` staging names.
    if let Some(name) = trimmed.rsplit('/').next() {
        if let Some((_, suffix)) = name.rsplit_once('#') {
            if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
                return Err(StorageClientError::InvalidPath {
                    message: format!(
                        "object key `{key}` uses reserved object_store staging suffix /#\\d+"
                    ),
                });
            }
        }
    }
    Path::parse(trimmed).map_err(|error| StorageClientError::InvalidPath {
        message: error.to_string(),
    })
}

fn optional_prefix(prefix: &str) -> StorageResult<Option<Path>> {
    let trimmed = prefix.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_key(trimmed)?))
}

fn map_put_error(key: &str, error: object_store::Error) -> StorageClientError {
    match error {
        object_store::Error::AlreadyExists { .. } => StorageClientError::AlreadyExists {
            key: key.to_string(),
        },
        object_store::Error::NotFound { .. } => StorageClientError::NotFound {
            key: key.to_string(),
        },
        other => StorageClientError::Backend {
            message: other.to_string(),
        },
    }
}

fn map_object_store_error_for_key(key: &str, error: object_store::Error) -> StorageClientError {
    match error {
        object_store::Error::NotFound { .. } => StorageClientError::NotFound {
            key: key.to_string(),
        },
        other => StorageClientError::Backend {
            message: other.to_string(),
        },
    }
}

fn map_object_store_error(error: object_store::Error) -> StorageClientError {
    StorageClientError::Backend {
        message: error.to_string(),
    }
}

fn object_store_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("koldstore-object-store")
            .enable_all()
            .build()
            .expect("create tokio runtime for object_store IO")
    })
}

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    // Flush/SPI paths are sync. Prefer a dedicated runtime so we never nest
    // `block_on` on a caller-owned tokio worker (which panics).
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| object_store_runtime().block_on(future))
    } else {
        object_store_runtime().block_on(future)
    }
}
