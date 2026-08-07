//! Client factory entry points for catalog and configured backends.

use std::time::Duration;

use crate::client::{
    ObjectStoreClient, PutPrecondition, StorageClient, StorageClientError, StorageResult,
};

use super::azure::open_azure_client;
use super::config::BackendConfig;
use super::fs::{ensure_filesystem_base_prepared, parse_filesystem_root};
use super::gcs::open_gcs_client;
use super::kind::StorageBackendKind;
use super::s3::open_s3_client;

/// Probe object key written then deleted during registration writability checks.
pub const STORAGE_WRITE_PROBE_KEY: &str = ".koldstore-write-probe";

/// Opens a durable storage client for the configured backend.
///
/// Filesystem backends use [`LocalFileSystem`](object_store::local::LocalFileSystem)
/// with `with_fsync(true)`. Cloud backends use the matching `object_store`
/// builder when the corresponding cargo feature is enabled:
/// - S3 / MinIO: [`AmazonS3Builder`](object_store::aws::AmazonS3Builder) (`s3`)
/// - GCS: [`GoogleCloudStorageBuilder`](object_store::gcp::GoogleCloudStorageBuilder) (`gcs`)
/// - Azure: [`MicrosoftAzureBuilder`](object_store::azure::MicrosoftAzureBuilder) (`azure`)
///
/// # Errors
///
/// Returns an error when the backend kind is unsupported, credentials are
/// missing, the required cargo feature is disabled, or the client cannot be
/// constructed.
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
/// missing, the required cargo feature is disabled, or the client cannot be
/// constructed.
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
        StorageBackendKind::Gcs => {
            open_gcs_client(&config.base_path, credentials, &config.config, timeout)?
        }
        StorageBackendKind::Azure => {
            open_azure_client(&config.base_path, credentials, &config.config, timeout)?
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

/// Verifies a storage backend can open and complete a put/delete probe.
///
/// Used by `register_storage` / `alter_storage_location` when `check => true`
/// (default). For filesystem backends, creates `base_path` first. For every
/// backend, writes then deletes [`STORAGE_WRITE_PROBE_KEY`] through the same
/// `object_store` client path flush will use.
///
/// # Errors
///
/// Returns an error when the client cannot be constructed or the probe write /
/// delete fails (permissions, credentials, network, missing bucket, …).
pub fn ensure_storage_backend_writable(
    storage_type: &str,
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
) -> StorageResult<()> {
    let kind = StorageBackendKind::parse(storage_type)
        .map_err(|message| StorageClientError::InvalidPath { message })?;
    if kind == StorageBackendKind::Filesystem {
        ensure_filesystem_base_prepared(base_path)?;
    }

    let client = open_client_from_catalog_fields(storage_type, base_path, credentials, config)
        .map_err(|error| StorageClientError::Backend {
            message: format!(
                "storage check failed opening {storage_type} backend at `{base_path}`: {error}"
            ),
        })?;

    let payload = b"koldstore-storage-check";
    client
        .put(STORAGE_WRITE_PROBE_KEY, payload, PutPrecondition::Overwrite)
        .map_err(|error| StorageClientError::Backend {
            message: format!(
                "storage check failed writing probe object `{STORAGE_WRITE_PROBE_KEY}` \
                 on {storage_type} backend at `{base_path}`: {error}"
            ),
        })?;
    if let Err(error) = client.delete(STORAGE_WRITE_PROBE_KEY) {
        return Err(StorageClientError::Backend {
            message: format!(
                "storage check wrote probe object but failed to delete `{STORAGE_WRITE_PROBE_KEY}` \
                 on {storage_type} backend at `{base_path}`: {error}"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ensure_storage_backend_writable, STORAGE_WRITE_PROBE_KEY};
    use crate::client::StorageClient;
    use crate::open_filesystem_client;

    #[test]
    fn ensure_storage_backend_writable_filesystem_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cold");
        ensure_storage_backend_writable(
            "filesystem",
            path.to_str().unwrap(),
            &serde_json::json!({}),
            &serde_json::json!({}),
        )
        .expect("writable");
        let client = open_filesystem_client(path.to_str().unwrap()).unwrap();
        assert!(client.head(STORAGE_WRITE_PROBE_KEY).is_err());
    }
}
