//! Managed table kind.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{KoldstoreError, Result};

/// Table management mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableKind {
    /// Shared logical table with one manifest scope.
    Shared,
    /// User-scoped logical table routed by `koldstore.user_id`.
    User,
}

impl TableKind {
    /// Returns the SQL contract string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::User => "user",
        }
    }
}

impl fmt::Display for TableKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TableKind {
    type Err = KoldstoreError;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "shared" => Ok(Self::Shared),
            "user" | "user_scoped" | "userscoped" => Ok(Self::User),
            other => Err(KoldstoreError::UnsupportedTableKind(other.to_string())),
        }
    }
}
