//! Storage client error types.

use thiserror::Error;

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
    /// Operation exceeded the configured ObjectStore timeout.
    #[error("object store operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
}
