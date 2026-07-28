//! Stable managed-column identity.
//!
//! Names are schema-version labels. Persisted metadata, indexes, and planning
//! use [`ColumnId`] (initialized from `pg_attribute.attnum`).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{KoldstoreError, Result};

/// Stable logical ID for a managed source-table column.
///
/// Initialized from PostgreSQL `pg_attribute.attnum` for the managed relation.
/// Renaming preserves the ID; dropping and recreating yields a new ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ColumnId(i16);

impl ColumnId {
    /// Creates a column ID from a PostgreSQL attribute number.
    ///
    /// User attributes are positive (`attnum >= 1`). System attributes use
    /// negative numbers and are out of scope for managed application columns.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero (invalid `attnum`).
    pub fn new(value: i16) -> Result<Self> {
        if value == 0 {
            return Err(KoldstoreError::InvalidIdentifier {
                kind: "column_id",
                value: "0".to_string(),
            });
        }
        Ok(Self(value))
    }

    /// Creates a column ID without validating (for trusted catalog/attnum paths).
    #[must_use]
    pub const fn from_attnum(value: i16) -> Self {
        Self(value)
    }

    /// Returns the raw attribute number.
    #[must_use]
    pub const fn get(self) -> i16 {
        self.0
    }
}

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Column reference with stable ID plus schema-version display name.
///
/// Persist and compare by [`column_id`](Self::column_id). Resolve `name` only at
/// SQL/UI boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Stable logical column ID (`pg_attribute.attnum`).
    pub column_id: ColumnId,
    /// Column name in the current schema version (diagnostics / SQL only).
    pub name: String,
}

impl ColumnRef {
    /// Creates a column reference from a trusted attribute number and name.
    #[must_use]
    pub fn new(column_id: ColumnId, name: impl Into<String>) -> Self {
        Self {
            column_id,
            name: name.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_id_rejects_zero() {
        assert!(ColumnId::new(0).is_err());
        assert_eq!(ColumnId::new(3).unwrap().get(), 3);
    }

    #[test]
    fn column_ref_serializes_id_and_name() {
        let reference = ColumnRef::new(ColumnId::from_attnum(3), "created_at");
        let value = serde_json::to_value(&reference).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "column_id": 3,
                "name": "created_at"
            })
        );
    }
}
