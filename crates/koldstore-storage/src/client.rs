//! Object storage client trait for list/put/get/delete operations.

use crate::object::StorageObject;
use thiserror::Error;

/// Storage client operation result.
pub type StorageResult<T> = Result<T, StorageClientError>;

/// Error returned by storage client implementations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StorageClientError {
    /// Object was not found.
    #[error("object not found: {key}")]
    NotFound { key: String },
    /// Backend rejected the request.
    #[error("storage backend error: {message}")]
    Backend { message: String },
}

/// Backend-agnostic object storage access.
pub trait StorageClient {
    /// Lists objects under a prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend request fails.
    fn list(&self, prefix: &str) -> StorageResult<Vec<StorageObject>>;

    /// Uploads object bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend request fails.
    fn put(&self, object: &StorageObject, bytes: &[u8]) -> StorageResult<()>;

    /// Downloads object bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the object is missing or the backend request fails.
    fn get(&self, key: &str) -> StorageResult<Vec<u8>>;

    /// Deletes one object.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend request fails.
    fn delete(&self, key: &str) -> StorageResult<()>;
}
