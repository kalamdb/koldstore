//! Sequence, commit-sequence, and scope-key newtypes.

use std::{fmt, num::NonZeroI64, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{KoldstoreError, Result};

/// Monotonic row/effect version id. Gaps are allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SeqId(NonZeroI64);

impl SeqId {
    /// Creates a positive sequence id.
    ///
    /// # Errors
    ///
    /// Returns an error when `value <= 0`.
    pub fn new(value: i64) -> Result<Self> {
        let Some(inner) = NonZeroI64::new(value) else {
            return Err(KoldstoreError::InvalidSequence {
                field: "seq",
                value,
            });
        };
        if inner.get() < 0 {
            return Err(KoldstoreError::InvalidSequence {
                field: "seq",
                value,
            });
        }
        Ok(Self(inner))
    }

    /// Returns the raw integer value.
    #[must_use]
    pub fn get(self) -> i64 {
        self.0.get()
    }
}

impl fmt::Display for SeqId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

/// Durable commit-order cursor. Gaps are allowed after rollbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CommitSeq(NonZeroI64);

impl CommitSeq {
    /// Creates a positive commit sequence.
    ///
    /// # Errors
    ///
    /// Returns an error when `value <= 0`.
    pub fn new(value: i64) -> Result<Self> {
        let Some(inner) = NonZeroI64::new(value) else {
            return Err(KoldstoreError::InvalidSequence {
                field: "commit_seq",
                value,
            });
        };
        if inner.get() < 0 {
            return Err(KoldstoreError::InvalidSequence {
                field: "commit_seq",
                value,
            });
        }
        Ok(Self(inner))
    }

    /// Returns the raw integer value.
    #[must_use]
    pub fn get(self) -> i64 {
        self.0.get()
    }
}

impl fmt::Display for CommitSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

/// Scope routing key for user-scoped managed tables.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ScopeKey(String);

impl ScopeKey {
    /// Creates a normalized non-empty scope key.
    ///
    /// # Errors
    ///
    /// Returns an error for empty or whitespace-only keys.
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let trimmed = value.as_ref().trim();
        if trimmed.is_empty() {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "scope key cannot be empty".to_string(),
            ));
        }
        Ok(Self(trimmed.to_string()))
    }

    /// Returns the scope as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ScopeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ScopeKey {
    type Err = KoldstoreError;

    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}
