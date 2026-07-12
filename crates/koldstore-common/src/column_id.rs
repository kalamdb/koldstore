//! Stable catalog column identifier.

use std::{fmt, num::NonZeroU64, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{KoldstoreError, Result};

/// Stable column identity allocated monotonically per managed table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ColumnId(NonZeroU64);

impl ColumnId {
    /// First valid column identifier.
    pub const NEXT_START: u64 = 1;

    /// Creates a positive column identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u64) -> Result<Self> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(KoldstoreError::InvalidColumnId(value))
    }

    /// Returns the raw identifier value.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

impl FromStr for ColumnId {
    type Err = KoldstoreError;

    fn from_str(value: &str) -> Result<Self> {
        let parsed = value
            .parse::<u64>()
            .map_err(|_| KoldstoreError::InvalidColumnId(0))?;
        Self::new(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_ids_are_positive_and_transparently_serialized() {
        let id = ColumnId::new(ColumnId::NEXT_START).unwrap();

        assert_eq!(id.get(), 1);
        assert_eq!(id.to_string(), "1");
        assert_eq!("1".parse::<ColumnId>().unwrap(), id);
        assert_eq!(serde_json::to_value(id).unwrap(), serde_json::json!(1));
        assert!(ColumnId::new(0).is_err());
    }
}
