//! Backend configuration parsed from catalog storage fields.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::client::{StorageClientError, StorageResult};

use super::fs::parse_filesystem_root;
use super::kind::StorageBackendKind;

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
                // Accept `file://…`, Unix absolute paths (`/…`, including when
                // validated on Windows), and native Windows absolute paths.
                base_path.starts_with("file://")
                    || base_path.starts_with('/')
                    || Path::new(&base_path).is_absolute()
            }
            StorageBackendKind::S3 => base_path.starts_with("s3://"),
            StorageBackendKind::Gcs => base_path.starts_with("gs://"),
            StorageBackendKind::Azure => {
                let lower = base_path.to_ascii_lowercase();
                lower.starts_with("azure://")
                    || lower.starts_with("az://")
                    || lower.starts_with("adl://")
                    || lower.starts_with("abfs://")
                    || lower.starts_with("abfss://")
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

#[cfg(test)]
mod tests {
    use super::{BackendConfig, StorageBackendKind};

    #[test]
    fn filesystem_accepts_unix_and_file_uri() {
        assert!(BackendConfig::new(
            StorageBackendKind::Filesystem,
            "/tmp/koldstore",
            serde_json::json!({}),
        )
        .is_ok());
        assert!(BackendConfig::new(
            StorageBackendKind::Filesystem,
            "file:///tmp/koldstore",
            serde_json::json!({}),
        )
        .is_ok());
    }

    #[test]
    fn filesystem_accepts_platform_absolute_paths() {
        #[cfg(windows)]
        let path = r"C:\Users\Jamal\AppData\Local\Temp\koldstore-test";
        #[cfg(not(windows))]
        let path = "/var/tmp/koldstore-test";
        assert!(
            BackendConfig::new(StorageBackendKind::Filesystem, path, serde_json::json!({}),)
                .is_ok(),
            "absolute path should be valid: {path}"
        );
    }

    #[test]
    fn filesystem_rejects_relative_paths() {
        assert!(BackendConfig::new(
            StorageBackendKind::Filesystem,
            "relative/path",
            serde_json::json!({}),
        )
        .is_err());
    }
}
