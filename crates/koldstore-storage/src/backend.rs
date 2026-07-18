//! Storage backend configuration and client factory.

use std::path::{Path, PathBuf};
#[cfg(feature = "s3")]
use std::sync::Arc;

#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "s3")]
use object_store::path::Path as ObjectPath;
#[cfg(feature = "s3")]
use object_store::prefix::PrefixStore;
#[cfg(feature = "s3")]
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::client::{ObjectStoreClient, StorageClientError, StorageResult};

/// Supported backend kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackendKind {
    Filesystem,
    S3,
    Gcs,
    Azure,
}

impl StorageBackendKind {
    /// Parses a catalog `storage_type` string.
    ///
    /// # Errors
    ///
    /// Returns an error when the type is unsupported.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "filesystem" | "file" | "local" => Ok(Self::Filesystem),
            "s3" | "aws" | "minio" => Ok(Self::S3),
            "gcs" | "gs" => Ok(Self::Gcs),
            "azure" | "abfs" => Ok(Self::Azure),
            other => Err(format!("unsupported storage_type `{other}`")),
        }
    }

    /// Catalog / SQL storage_type label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filesystem => "filesystem",
            Self::S3 => "s3",
            Self::Gcs => "gcs",
            Self::Azure => "azure",
        }
    }
}

/// Backend config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendConfig {
    pub kind: StorageBackendKind,
    pub base_path: String,
    pub config: serde_json::Value,
}

impl BackendConfig {
    /// Creates and validates backend configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the base path scheme does not match the backend kind.
    pub fn new(
        kind: StorageBackendKind,
        base_path: impl Into<String>,
        config: serde_json::Value,
    ) -> Result<Self, String> {
        let base_path = base_path.into();
        let valid = match kind {
            StorageBackendKind::Filesystem => {
                base_path.starts_with("file://") || base_path.starts_with('/')
            }
            StorageBackendKind::S3 => base_path.starts_with("s3://"),
            StorageBackendKind::Gcs => base_path.starts_with("gs://"),
            StorageBackendKind::Azure => {
                base_path.starts_with("azure://") || base_path.starts_with("abfs://")
            }
        };
        if !valid {
            return Err(format!(
                "base path {base_path:?} is not valid for {kind:?} backend"
            ));
        }
        Ok(Self {
            kind,
            base_path,
            config,
        })
    }

    /// Resolves a filesystem `base_path` (`file://…` or absolute path) to a [`PathBuf`].
    ///
    /// # Errors
    ///
    /// Returns an error when the scheme is not filesystem-compatible.
    pub fn filesystem_root(&self) -> StorageResult<PathBuf> {
        match self.kind {
            StorageBackendKind::Filesystem => parse_filesystem_root(&self.base_path),
            other => Err(StorageClientError::InvalidPath {
                message: format!("filesystem_root is not supported for {other:?}"),
            }),
        }
    }
}

/// Backend factory result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageBackend {
    pub config: BackendConfig,
}

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
    match config.kind {
        StorageBackendKind::Filesystem => {
            let root = config.filesystem_root()?;
            ObjectStoreClient::local_filesystem(root)
        }
        StorageBackendKind::S3 => open_s3_client(&config.base_path, credentials, &config.config),
        StorageBackendKind::Gcs | StorageBackendKind::Azure => Err(StorageClientError::Backend {
            message: format!(
                "{:?} client open is not wired yet (base_path={})",
                config.kind, config.base_path
            ),
        }),
    }
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
    let kind = StorageBackendKind::parse(storage_type)
        .map_err(|message| StorageClientError::InvalidPath { message })?;
    let backend = BackendConfig::new(kind, base_path, config.clone())
        .map_err(|message| StorageClientError::InvalidPath { message })?;
    open_storage_client(&backend, credentials)
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

#[cfg(feature = "s3")]
fn open_s3_client(
    base_path: &str,
    credentials: &serde_json::Value,
    config: &serde_json::Value,
) -> StorageResult<ObjectStoreClient> {
    crate::ensure_rustls_ring_provider();
    let (bucket, key_prefix) = parse_s3_base_path(base_path)?;
    let access_key = json_string(credentials, "access_key_id")
        .or_else(|| json_string(credentials, "access_key"));
    let secret_key = json_string(credentials, "secret_access_key")
        .or_else(|| json_string(credentials, "secret_key"));
    let (access_key, secret_key) = match (access_key, secret_key) {
        (Some(access), Some(secret)) => (access, secret),
        _ => {
            return Err(StorageClientError::Backend {
                message: "s3 credentials require access_key_id and secret_access_key".to_string(),
            })
        }
    };
    let region = json_string(config, "region").unwrap_or_else(|| "us-east-1".to_string());
    let endpoint = json_string(config, "endpoint");
    let path_style = json_bool(config, "path_style").unwrap_or(endpoint.is_some());
    let allow_http = endpoint
        .as_deref()
        .is_some_and(|value| value.starts_with("http://"));

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_virtual_hosted_style_request(!path_style)
        .with_allow_http(allow_http);
    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    let store = builder
        .build()
        .map_err(|error| StorageClientError::Backend {
            message: error.to_string(),
        })?;

    let store: Arc<dyn ObjectStore> = if key_prefix.is_empty() {
        Arc::new(store)
    } else {
        let prefix =
            ObjectPath::parse(&key_prefix).map_err(|error| StorageClientError::InvalidPath {
                message: error.to_string(),
            })?;
        Arc::new(PrefixStore::new(store, prefix))
    };
    Ok(ObjectStoreClient::from_store(store, None))
}

#[cfg(not(feature = "s3"))]
fn open_s3_client(
    _base_path: &str,
    _credentials: &serde_json::Value,
    _config: &serde_json::Value,
) -> StorageResult<ObjectStoreClient> {
    Err(StorageClientError::Backend {
        message: "s3 backend requires the `s3` cargo feature (not enabled in this build)"
            .to_string(),
    })
}

/// Parses `s3://bucket` or `s3://bucket/optional/prefix` into bucket + prefix.
#[cfg(any(feature = "s3", test))]
fn parse_s3_base_path(base_path: &str) -> StorageResult<(String, String)> {
    let rest = base_path
        .strip_prefix("s3://")
        .ok_or_else(|| StorageClientError::InvalidPath {
            message: format!("s3 base_path must start with s3://: {base_path}"),
        })?;
    let rest = rest.trim_matches('/');
    if rest.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: format!("s3 base_path missing bucket: {base_path}"),
        });
    }
    let (bucket, prefix) = match rest.split_once('/') {
        Some((bucket, prefix)) => (bucket.to_string(), prefix.trim_matches('/').to_string()),
        None => (rest.to_string(), String::new()),
    };
    if bucket.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: format!("s3 base_path missing bucket: {base_path}"),
        });
    }
    Ok((bucket, prefix))
}

fn parse_filesystem_root(base_path: &str) -> StorageResult<PathBuf> {
    let trimmed = base_path.trim();
    if trimmed.is_empty() {
        return Err(StorageClientError::InvalidPath {
            message: "filesystem base_path must not be empty".to_string(),
        });
    }
    if let Some(rest) = trimmed.strip_prefix("file://") {
        let path = rest.strip_prefix("localhost").unwrap_or(rest);
        if path.is_empty() {
            return Err(StorageClientError::InvalidPath {
                message: format!("invalid file URI: {base_path}"),
            });
        }
        return Ok(PathBuf::from(path));
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Err(StorageClientError::InvalidPath {
            message: format!("filesystem base_path must be absolute or file:// URI: {base_path}"),
        })
    }
}

#[cfg(feature = "s3")]
fn json_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

#[cfg(feature = "s3")]
fn json_bool(value: &serde_json::Value, key: &str) -> Option<bool> {
    match value.get(key)? {
        serde_json::Value::Bool(flag) => Some(*flag),
        serde_json::Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_s3_base_path, StorageBackendKind};

    #[test]
    fn parses_s3_bucket_and_prefix() {
        assert_eq!(
            parse_s3_base_path("s3://koldstore-test").unwrap(),
            ("koldstore-test".to_string(), String::new())
        );
        assert_eq!(
            parse_s3_base_path("s3://koldstore-test/prod/").unwrap(),
            ("koldstore-test".to_string(), "prod".to_string())
        );
    }

    #[test]
    fn storage_backend_kind_accepts_minio_alias() {
        assert_eq!(
            StorageBackendKind::parse("minio").unwrap(),
            StorageBackendKind::S3
        );
    }
}
