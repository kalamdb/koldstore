//! PostgreSQL table OID newtype for pg-free crates.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{KoldstoreError, Result};

/// Stable PostgreSQL relation OID (`pg_class.oid`) without linking `pg_sys`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableOid(u32);

impl TableOid {
    /// Creates a table OID.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero (`InvalidOid`).
    pub fn new(value: u32) -> Result<Self> {
        if value == 0 {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "table oid must be greater than zero".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Wraps a non-zero OID without validation (caller already checked).
    #[must_use]
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    /// Returns the raw OID value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for TableOid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::TableOid;

    #[test]
    fn rejects_invalid_oid() {
        assert!(TableOid::new(0).is_err());
        assert_eq!(TableOid::new(42).unwrap().get(), 42);
    }
}
