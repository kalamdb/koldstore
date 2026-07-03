//! Logical primary-key encoding and stable hashing.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{KoldstoreError, Result};

/// A primary-key column name in logical order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PkColumn(String);

impl PkColumn {
    /// Creates a non-empty primary-key column name.
    ///
    /// # Errors
    ///
    /// Returns an error when the name is empty.
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let trimmed = value.as_ref().trim();
        if trimmed.is_empty() {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary-key column name cannot be empty".to_string(),
            ));
        }
        Ok(Self(trimmed.to_string()))
    }

    /// Returns the column name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PkColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A JSON-compatible primary-key value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PkValue(Value);

impl PkValue {
    /// Creates a non-null primary-key value.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is null.
    pub fn new(value: Value) -> Result<Self> {
        if value.is_null() {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary-key value cannot be null".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the underlying JSON value.
    #[must_use]
    pub fn as_json(&self) -> &Value {
        &self.0
    }
}

/// Ordered logical primary key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalPk {
    columns: Vec<(PkColumn, PkValue)>,
}

impl LogicalPk {
    /// Creates a logical primary key from ordered pairs.
    ///
    /// # Errors
    ///
    /// Returns an error when no columns are supplied or duplicate columns exist.
    pub fn new(columns: Vec<(PkColumn, PkValue)>) -> Result<Self> {
        if columns.is_empty() {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary key must include at least one column".to_string(),
            ));
        }
        let mut seen = BTreeMap::new();
        for (idx, (column, _)) in columns.iter().enumerate() {
            if seen.insert(column.as_str(), idx).is_some() {
                return Err(KoldstoreError::InvalidPrimaryKey(format!(
                    "duplicate primary-key column: {}",
                    column
                )));
            }
        }
        Ok(Self { columns })
    }

    /// Builds a logical key from a JSON object and ordered column names.
    ///
    /// # Errors
    ///
    /// Returns an error if a column is missing, null, duplicated, or if `pk` is not an object.
    pub fn from_json_object(pk: &Value, ordered_columns: &[PkColumn]) -> Result<Self> {
        let Some(object) = pk.as_object() else {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary-key JSON must be an object".to_string(),
            ));
        };
        let mut pairs = Vec::with_capacity(ordered_columns.len());
        for column in ordered_columns {
            let value = object.get(column.as_str()).ok_or_else(|| {
                KoldstoreError::InvalidPrimaryKey(format!(
                    "primary-key JSON missing column: {}",
                    column
                ))
            })?;
            pairs.push((column.clone(), PkValue::new(value.clone())?));
        }
        Self::new(pairs)
    }

    /// Returns the ordered key pairs.
    #[must_use]
    pub fn columns(&self) -> &[(PkColumn, PkValue)] {
        &self.columns
    }

    /// Encodes the primary key as canonical JSON with ordered keys.
    #[must_use]
    pub fn to_canonical_json(&self) -> Value {
        let mut map = serde_json::Map::new();
        for (column, value) in &self.columns {
            map.insert(column.as_str().to_string(), value.as_json().clone());
        }
        Value::Object(map)
    }
}

/// Stable SHA-256 hash of a logical primary key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StablePkHash(String);

impl StablePkHash {
    /// Creates a stable PK hash from an existing non-empty digest string.
    ///
    /// # Errors
    ///
    /// Returns an error when the hash is empty or whitespace-only.
    pub fn from_hex(value: impl AsRef<str>) -> Result<Self> {
        let trimmed = value.as_ref().trim();
        if trimmed.is_empty() {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary-key hash cannot be empty".to_string(),
            ));
        }
        Ok(Self(trimmed.to_string()))
    }

    /// Computes the stable hex hash for a logical PK.
    #[must_use]
    pub fn compute(pk: &LogicalPk) -> Self {
        let mut hasher = Sha256::new();
        for (column, value) in pk.columns() {
            hasher.update(column.as_str().as_bytes());
            hasher.update([0]);
            hasher.update(value.as_json().to_string().as_bytes());
            hasher.update([0xff]);
        }
        Self(hex::encode(hasher.finalize()))
    }

    /// Returns the hex-encoded digest.
    #[must_use]
    pub fn as_hex(&self) -> &str {
        &self.0
    }
}
