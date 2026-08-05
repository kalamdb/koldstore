//! Storage backend kind labels used by catalog `storage_type`.

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
            "gcs" | "gs" | "gcp" => Ok(Self::Gcs),
            "azure" | "az" | "abfs" | "abfss" | "adl" => Ok(Self::Azure),
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

#[cfg(test)]
mod tests {
    use super::StorageBackendKind;

    #[test]
    fn storage_backend_kind_accepts_cloud_aliases() {
        assert_eq!(
            StorageBackendKind::parse("minio").unwrap(),
            StorageBackendKind::S3
        );
        assert_eq!(
            StorageBackendKind::parse("gcp").unwrap(),
            StorageBackendKind::Gcs
        );
        assert_eq!(
            StorageBackendKind::parse("az").unwrap(),
            StorageBackendKind::Azure
        );
        assert_eq!(
            StorageBackendKind::parse("abfss").unwrap(),
            StorageBackendKind::Azure
        );
    }
}
