//! Storage backend configuration.

use serde::{Deserialize, Serialize};

/// Supported backend kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackendKind {
    Filesystem,
    S3,
    Gcs,
    Azure,
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
}

/// Backend factory result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageBackend {
    pub config: BackendConfig,
}
