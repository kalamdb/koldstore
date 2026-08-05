//! Backend-agnostic storage client trait and put types.

use crate::object::StorageObject;

use super::error::StorageResult;

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
    /// Returns [`super::error::StorageClientError::AlreadyExists`] when `to`
    /// exists, or a backend error on failure.
    fn copy_if_absent(&self, from: &str, to: &str) -> StorageResult<()>;
}
