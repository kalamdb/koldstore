//! Sort Key V1 errors.

use thiserror::Error;

/// Encoding or decoding failure for Sort Key V1.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SortKeyError {
    /// PostgreSQL type OID is outside the Sort Key V1 allowlist.
    #[error("unsupported sort-key type oid {0}")]
    UnsupportedTypeOid(u32),
    /// JSON value shape does not match the declared sort-key type.
    #[error("sort-key JSON value does not match type {expected}: {detail}")]
    InvalidJson {
        /// Expected Sort Key V1 type name.
        expected: &'static str,
        /// Parse detail.
        detail: String,
    },
    /// Storekey rejected the value.
    #[error("storekey encode error: {0}")]
    Encode(String),
    /// Storekey could not decode the bytes as the declared type.
    #[error("storekey decode error: {0}")]
    Decode(String),
}
