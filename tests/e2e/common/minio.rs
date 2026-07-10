//! Opt-in MinIO helpers for S3-backed E2E fixtures.
//!
//! Enabled when `KOLDSTORE_MINIO=1` (or `KOLDSTORE_MINIO_ENDPOINT` is set).
//! Defaults match `docker/run.sh` and `crates/koldstore-storage/tests/storage_minio.rs`.

use anyhow::{Context, Result};
use koldstore_storage::{
    open_client_from_catalog_fields, ObjectStoreClient, StorageClient, StorageClientError,
};
use serde_json::json;

/// Connection settings for a local MinIO (S3-compatible) endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinioConfig {
    /// HTTP(S) endpoint, for example `http://127.0.0.1:19000`.
    pub endpoint: String,
    /// Access key id.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: String,
    /// Bucket name (must already exist).
    pub bucket: String,
}

impl MinioConfig {
    /// Loads MinIO settings from the environment when the opt-in gate is set.
    ///
    /// Returns `None` when MinIO-backed tests should be skipped.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let enabled = std::env::var("KOLDSTORE_MINIO").ok().as_deref() == Some("1");
        let endpoint = std::env::var("KOLDSTORE_MINIO_ENDPOINT").ok();
        if !enabled && endpoint.is_none() {
            return None;
        }
        Some(Self {
            endpoint: endpoint.unwrap_or_else(|| "http://127.0.0.1:19000".to_string()),
            access_key: std::env::var("KOLDSTORE_MINIO_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            secret_key: std::env::var("KOLDSTORE_MINIO_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            bucket: std::env::var("KOLDSTORE_MINIO_BUCKET")
                .unwrap_or_else(|_| "koldstore-test".to_string()),
        })
    }

    /// Returns MinIO config or an error explaining how to enable the tests.
    ///
    /// # Errors
    ///
    /// Returns an error when the opt-in env gate is unset.
    pub fn require() -> Result<Self> {
        Self::from_env().context(
            "MinIO E2E tests require KOLDSTORE_MINIO=1 (and a reachable MinIO). \
             Start MinIO via docker/run.sh or set KOLDSTORE_MINIO_ENDPOINT",
        )
    }

    /// S3 `base_path` for a fixture-scoped prefix under the configured bucket.
    #[must_use]
    pub fn base_path_for_prefix(&self, object_prefix: &str) -> String {
        let prefix = object_prefix.trim_matches('/');
        if prefix.is_empty() {
            format!("s3://{}/", self.bucket)
        } else {
            format!("s3://{}/{}/", self.bucket, prefix)
        }
    }

    /// JSON credentials payload for `koldstore.register_storage`.
    #[must_use]
    pub fn credentials_json(&self) -> serde_json::Value {
        json!({
            "access_key_id": self.access_key,
            "secret_access_key": self.secret_key,
        })
    }

    /// JSON config payload for `koldstore.register_storage` (endpoint + path-style).
    #[must_use]
    pub fn config_json(&self) -> serde_json::Value {
        json!({
            "endpoint": self.endpoint,
            "region": "us-east-1",
            "path_style": true,
        })
    }

    /// Opens an object-store client rooted at `s3://bucket/{object_prefix}/`.
    ///
    /// # Errors
    ///
    /// Returns an error when the client cannot be opened.
    pub fn open_client(&self, object_prefix: &str) -> Result<ObjectStoreClient> {
        let base_path = self.base_path_for_prefix(object_prefix);
        open_client_from_catalog_fields(
            "s3",
            &base_path,
            &self.credentials_json(),
            &self.config_json(),
        )
        .with_context(|| format!("open MinIO client at {} for {}", self.endpoint, base_path))
    }

    /// Probes MinIO with a list of the fixture prefix (creates no objects).
    ///
    /// # Errors
    ///
    /// Returns an error when MinIO is unreachable or credentials/bucket are wrong.
    pub fn probe(&self, object_prefix: &str) -> Result<()> {
        let client = self.open_client(object_prefix)?;
        client.list("").map(|_| ()).map_err(|error| match error {
            StorageClientError::NotFound { .. } => anyhow::anyhow!(
                "MinIO bucket '{}' is missing; create it (docker/run.sh or mc mb)",
                self.bucket
            ),
            other => anyhow::anyhow!("MinIO probe failed against {}: {other}", self.endpoint),
        })
    }
}

/// Returns true when MinIO-backed E2E tests should run.
#[must_use]
pub fn minio_enabled() -> bool {
    MinioConfig::from_env().is_some()
}
