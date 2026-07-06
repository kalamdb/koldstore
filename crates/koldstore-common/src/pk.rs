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

/// One-based ordinal of a primary-key column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PkOrdinal(u16);

impl PkOrdinal {
    /// Creates a one-based primary-key ordinal.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u16) -> Result<Self> {
        if value == 0 {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary-key ordinal must be greater than zero".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the one-based ordinal value.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// PostgreSQL type OID for a primary-key column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PgTypeOid(u32);

impl PgTypeOid {
    /// Creates a PostgreSQL type OID.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u32) -> Result<Self> {
        if value == 0 {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary-key type oid must be greater than zero".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the raw OID value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// PostgreSQL type name for a primary-key column.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PgTypeName(String);

impl PgTypeName {
    /// Creates a non-empty PostgreSQL type name.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is empty.
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let trimmed = value.as_ref().trim();
        if trimmed.is_empty() {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary-key type name cannot be empty".to_string(),
            ));
        }
        Ok(Self(trimmed.to_string()))
    }

    /// Returns the type name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// PostgreSQL type modifier for a primary-key column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PgTypmod(i32);

impl PgTypmod {
    /// Creates a PostgreSQL type modifier. `-1` means no typmod.
    #[must_use]
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    /// Returns the raw typmod value.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// PostgreSQL collation identity for a collatable primary-key column.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PgCollation(String);

impl PgCollation {
    /// Creates a non-empty collation identity.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is empty.
    pub fn new(value: impl AsRef<str>) -> Result<Self> {
        let trimmed = value.as_ref().trim();
        if trimmed.is_empty() {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary-key collation cannot be empty".to_string(),
            ));
        }
        Ok(Self(trimmed.to_string()))
    }

    /// Returns the collation identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact source-table primary-key column shape preserved by clean-schema mirrors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryKeyColumnShape {
    column: PkColumn,
    ordinal: PkOrdinal,
    type_oid: PgTypeOid,
    type_name: PgTypeName,
    typmod: PgTypmod,
    collation: Option<PgCollation>,
    domain_identity: Option<PgTypeName>,
    not_null: bool,
}

impl PrimaryKeyColumnShape {
    /// Creates primary-key column shape metadata.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        column: PkColumn,
        ordinal: PkOrdinal,
        type_oid: PgTypeOid,
        type_name: PgTypeName,
        typmod: PgTypmod,
        collation: Option<PgCollation>,
        domain_identity: Option<PgTypeName>,
        not_null: bool,
    ) -> Self {
        Self {
            column,
            ordinal,
            type_oid,
            type_name,
            typmod,
            collation,
            domain_identity,
            not_null,
        }
    }

    /// Returns the primary-key column name.
    #[must_use]
    pub const fn column(&self) -> &PkColumn {
        &self.column
    }

    /// Returns the primary-key ordinal.
    #[must_use]
    pub const fn ordinal(&self) -> PkOrdinal {
        self.ordinal
    }

    /// Returns the PostgreSQL type OID.
    #[must_use]
    pub const fn type_oid(&self) -> PgTypeOid {
        self.type_oid
    }

    /// Returns the PostgreSQL type name.
    #[must_use]
    pub const fn type_name(&self) -> &PgTypeName {
        &self.type_name
    }

    /// Returns the PostgreSQL typmod.
    #[must_use]
    pub const fn typmod(&self) -> PgTypmod {
        self.typmod
    }

    /// Returns the collation identity for collatable keys.
    #[must_use]
    pub const fn collation(&self) -> Option<&PgCollation> {
        self.collation.as_ref()
    }

    /// Returns the domain identity when the key type is domain-backed.
    #[must_use]
    pub const fn domain_identity(&self) -> Option<&PgTypeName> {
        self.domain_identity.as_ref()
    }

    /// Returns whether the key column is not nullable.
    #[must_use]
    pub const fn not_null(&self) -> bool {
        self.not_null
    }
}

/// Ordered primary-key shape for a clean-schema managed table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryKeyShape {
    columns: Vec<PrimaryKeyColumnShape>,
}

impl PrimaryKeyShape {
    /// Creates an ordered primary-key shape.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty shape, duplicate columns, duplicate ordinals,
    /// or ordinals that do not match the supplied order.
    pub fn new(columns: Vec<PrimaryKeyColumnShape>) -> Result<Self> {
        if columns.is_empty() {
            return Err(KoldstoreError::InvalidPrimaryKey(
                "primary-key shape must include at least one column".to_string(),
            ));
        }

        let mut seen_columns = BTreeMap::new();
        let mut seen_ordinals = BTreeMap::new();
        for (idx, column) in columns.iter().enumerate() {
            let expected = u16::try_from(idx + 1).map_err(|_| {
                KoldstoreError::InvalidPrimaryKey(
                    "primary-key shape has too many columns".to_string(),
                )
            })?;
            if column.ordinal().get() != expected {
                return Err(KoldstoreError::InvalidPrimaryKey(format!(
                    "primary-key ordinal {} does not match position {}",
                    column.ordinal().get(),
                    expected
                )));
            }
            if seen_columns
                .insert(column.column().as_str(), column.ordinal().get())
                .is_some()
            {
                return Err(KoldstoreError::InvalidPrimaryKey(format!(
                    "duplicate primary-key column: {}",
                    column.column()
                )));
            }
            if seen_ordinals
                .insert(column.ordinal().get(), column.column().as_str())
                .is_some()
            {
                return Err(KoldstoreError::InvalidPrimaryKey(format!(
                    "duplicate primary-key ordinal: {}",
                    column.ordinal().get()
                )));
            }
        }

        Ok(Self { columns })
    }

    /// Returns primary-key columns in source-table order.
    #[must_use]
    pub fn columns(&self) -> &[PrimaryKeyColumnShape] {
        &self.columns
    }

    /// Returns the ordered column names.
    #[must_use]
    pub fn ordered_columns(&self) -> Vec<PkColumn> {
        self.columns
            .iter()
            .map(|column| column.column().clone())
            .collect()
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
