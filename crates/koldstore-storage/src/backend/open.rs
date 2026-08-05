//! Client factory entry points for catalog and configured backends.

use std::time::Duration;

use crate::client::{ObjectStoreClient, StorageClientError, StorageResult};

use super::config::BackendConfig;
use super::fs::parse_filesystem_root;
use super::kind::StorageBackendKind;
use super::s3::open_s3_client;

/// Opens a durable storage client for the configured backend.
///
/// Filesystem backends use [`LocalFileSystem`](object_store::local::LocalFileSystem)
/// with `with_fsync(true)`. S3-compatible backends (including MinIO) use
/// [`AmazonS3Builder`](object_store::aws::AmazonS3Builder) when the `s3`
/// cargo feature is enabled.
///
/// # Errors
///
/// Returns an error when the backend kind is unsupported, credentials are
/// missing, the `s3` feature is disabled for an S3 config, or the client
/// cannot be constructed.
pub fn open_storage_client(
    config: &BackendConfig,
    credentials: &serde_json::Value,
) -> StorageResult<ObjectStoreClient> {
    open_storage_client_with_timeout(config, credentials, None)
}

/// Like [`open_storage_client`], applying an optional outer operation timeout.
///
/// # Errors
///
/// Returns an error when the backend kind is unsupported, credentials are
/// missing, the `s3` feature is disabled for an S3 config, or the client
/// cannot be constructed.
pub fn open_storage_client_with_timeout(
    config: &BackendConfig,
    credentials: &serde_json::Value,
    timeout: Option<Duration>,
) -> StorageResult<ObjectStoreClient> {
    let client = match config.kind {
        StorageBackendKind::Filesystem => {
            let root = config.filesystem_root()?;
            ObjectStoreClient::local_filesystem(root)?
        }
        StorageBackendKind::S3 => {
            open_s3_client(&config.base_path, credentials, &config.config, timeout)?
        }
        StorageBackendKind::Gcs | StorageBackendKind::Azure => {
            return Err(StorageClientError::Backend {
                message: format!(
                    "{:?} client open is not wired yet (base_path={})",
                    config.kind, config.base_path
                ),
            })
        }
    };
    Ok(client.with_timeout(timeout))
}

/// Opens a client from catalog storage fields (`storage_type`, `base_path`, …).
///
/// # Errors
///
/// Returns an error when the type/path/credentials cannot open a client.
pub fn open_client_from_catalog_fields(
    storage_type: &str,
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
) -> StorageResult<ObjectStoreClient> {
    open_client_from_catalog_fields_with_timeout(storage_type, base_path, credentials, config, None)
}

/// Like [`open_client_from_catalog_fields`], applying an optional outer timeout.
///
/// # Errors
///
/// Returns an error when the type/path/credentials cannot open a client.
pub fn open_client_from_catalog_fields_with_timeout(
    storage_type: &str,
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
    timeout: Option<Duration>,
) -> StorageResult<ObjectStoreClient> {
    let kind = StorageBackendKind::parse(storage_type)
        .map_err(|message| StorageClientError::InvalidPath { message })?;
    let backend = BackendConfig::new(kind, base_path, config.clone())
        .map_err(|message| StorageClientError::InvalidPath { message })?;
    open_storage_client_with_timeout(&backend, credentials, timeout)
}

/// Opens a filesystem client from a raw base path string.
///
/// Accepts absolute paths and `file://` URIs.
///
/// # Errors
///
/// Returns an error when the path cannot be parsed or the root cannot be opened.
pub fn open_filesystem_client(base_path: impl AsRef<str>) -> StorageResult<ObjectStoreClient> {
    let root = parse_filesystem_root(base_path.as_ref())?;
    ObjectStoreClient::local_filesystem(root)
}
