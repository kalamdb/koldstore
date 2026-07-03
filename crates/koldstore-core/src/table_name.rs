//! Type-safe PostgreSQL table names.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{KoldstoreError, Result};

/// A validated one- or two-part PostgreSQL table name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableName(String);

impl TableName {
    /// Parses a one- or two-part unquoted PostgreSQL table name.
    ///
    /// # Errors
    ///
    /// Returns an error when the table name is blank, multipart beyond
    /// `schema.table`, or contains unsafe identifier characters.
    pub fn parse(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref().trim();
        let parts = value.split('.').collect::<Vec<_>>();
        let valid = match parts.as_slice() {
            [name] => is_safe_identifier(name),
            [schema, name] => is_safe_identifier(schema) && is_safe_identifier(name),
            _ => false,
        };

        if valid {
            Ok(Self(value.to_string()))
        } else {
            Err(KoldstoreError::InvalidIdentifier {
                kind: "table name",
                value: value.to_string(),
            })
        }
    }

    /// Returns the normalized table name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the optional schema component.
    #[must_use]
    pub fn schema(&self) -> Option<&str> {
        self.0.split_once('.').map(|(schema, _)| schema)
    }

    /// Returns the relation component.
    #[must_use]
    pub fn relation(&self) -> &str {
        self.0
            .split_once('.')
            .map_or(self.0.as_str(), |(_, relation)| relation)
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TableName {
    type Err = KoldstoreError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
