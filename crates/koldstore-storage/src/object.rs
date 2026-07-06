//! Object metadata stored alongside remote storage keys.

use serde::{Deserialize, Serialize};

/// Addressable object in remote storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageObject {
    /// Object key relative to the storage backend base path.
    pub key: String,
    /// Optional content etag or generation identifier.
    pub etag: Option<String>,
    /// Object size in bytes when known.
    pub byte_size: Option<u64>,
}
