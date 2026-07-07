//! Supported PostgreSQL column types for the pg-koldstore MVP.
//!
//! [`PgType`] is the single canonical enum for supported PostgreSQL column types.
//! Catalog-name normalization lives in `koldstore_common::canonical_postgres_type_name`;
//! Arrow and JSON value coercion live in `koldstore_parquet::pg_type_codec`.

use koldstore_common::canonical_postgres_type_name;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::TypeMatrix;

/// Supported PostgreSQL column type for managed tables and cold storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PgType {
    /// `bool`
    Bool = 0,
    /// `int2`
    Int2 = 1,
    /// `int4`
    Int4 = 2,
    /// `int8`
    Int8 = 3,
    /// `float4`
    Float4 = 4,
    /// `float8`
    Float8 = 5,
    /// `text` / `varchar`
    Text = 6,
    /// `numeric`
    Numeric = 7,
    /// `uuid`
    Uuid = 8,
    /// `jsonb`
    Jsonb = 9,
    /// `text[]`
    TextArray = 10,
    /// `bytea`
    Bytea = 11,
    /// `timestamptz`
    Timestamptz = 12,
}

/// PostgreSQL integer array type OID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgIntegerArrayOid {
    /// `smallint[]`
    Int2 = 1005,
    /// `integer[]`
    Int4 = 1007,
    /// `bigint[]`
    Int8 = 1016,
}

/// Schema/type conversion error.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    /// Catalog type is outside the MVP support matrix.
    #[error("unsupported PostgreSQL type: {0}")]
    UnsupportedType(String),
}

impl Serialize for PgType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.catalog_type_name())
    }
}

impl<'de> Deserialize<'de> for PgType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let type_name = String::deserialize(deserializer)?;
        Self::from_postgres_name(&type_name).map_err(serde::de::Error::custom)
    }
}

impl PgType {
    /// Parses PostgreSQL catalog type names and common aliases.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnsupportedType`] for unsupported or blank types.
    pub fn from_postgres_name(type_name: &str) -> Result<Self, SchemaError> {
        match canonical_postgres_type_name(type_name).as_str() {
            "bool" => Ok(Self::Bool),
            "int2" => Ok(Self::Int2),
            "int4" => Ok(Self::Int4),
            "int8" => Ok(Self::Int8),
            "float4" => Ok(Self::Float4),
            "float8" => Ok(Self::Float8),
            "text" | "varchar" => Ok(Self::Text),
            "numeric" => Ok(Self::Numeric),
            "uuid" => Ok(Self::Uuid),
            "jsonb" => Ok(Self::Jsonb),
            "text[]" => Ok(Self::TextArray),
            "bytea" => Ok(Self::Bytea),
            "timestamptz" => Ok(Self::Timestamptz),
            normalized => Err(SchemaError::UnsupportedType(normalized.to_string())),
        }
    }

    /// Returns the compact discriminant for this type.
    #[must_use]
    pub const fn discriminant(self) -> u8 {
        self as u8
    }

    /// Maps a compact discriminant back to a supported type.
    #[must_use]
    pub const fn from_discriminant(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Bool),
            1 => Some(Self::Int2),
            2 => Some(Self::Int4),
            3 => Some(Self::Int8),
            4 => Some(Self::Float4),
            5 => Some(Self::Float8),
            6 => Some(Self::Text),
            7 => Some(Self::Numeric),
            8 => Some(Self::Uuid),
            9 => Some(Self::Jsonb),
            10 => Some(Self::TextArray),
            11 => Some(Self::Bytea),
            12 => Some(Self::Timestamptz),
            _ => None,
        }
    }

    /// Returns the canonical MVP type name for diagnostics and matrix checks.
    #[must_use]
    pub const fn catalog_type_name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int2 => "int2",
            Self::Int4 => "int4",
            Self::Int8 => "int8",
            Self::Float4 => "float4",
            Self::Float8 => "float8",
            Self::Text => "text",
            Self::Numeric => "numeric",
            Self::Uuid => "uuid",
            Self::Jsonb => "jsonb",
            Self::TextArray => "text[]",
            Self::Bytea => "bytea",
            Self::Timestamptz => "timestamptz",
        }
    }

    /// Returns true when the type is supported by the MVP type matrix.
    #[must_use]
    pub fn is_mvp_supported(self) -> bool {
        TypeMatrix::postgres_15_default()
            .support_for(self.catalog_type_name())
            .supported
    }

    /// Returns true when the type can provide oldest-to-newest migration ordering.
    #[must_use]
    pub fn is_orderable(self) -> bool {
        matches!(
            self,
            Self::Int2 | Self::Int4 | Self::Int8 | Self::Timestamptz
        )
    }

    /// Returns true when a raw catalog type name can provide migration ordering.
    #[must_use]
    pub fn is_orderable_catalog_type(type_name: &str) -> bool {
        matches!(
            canonical_postgres_type_name(type_name).as_str(),
            "int2" | "int4" | "int8" | "timestamptz" | "timestamp" | "date"
        )
    }

    /// Maps a supported integer type to its PostgreSQL type OID.
    #[must_use]
    pub const fn integer_oid(self) -> Option<u32> {
        match self {
            Self::Int2 => Some(21),
            Self::Int4 => Some(23),
            Self::Int8 => Some(20),
            _ => None,
        }
    }

    /// Maps a PostgreSQL integer type OID to the supported `PgType`.
    #[must_use]
    pub fn from_integer_oid(oid: u32) -> Option<Self> {
        match oid {
            20 => Some(Self::Int8),
            21 => Some(Self::Int2),
            23 => Some(Self::Int4),
            _ => None,
        }
    }

    /// Maps a supported integer type to its PostgreSQL array type OID.
    #[must_use]
    pub const fn integer_array_oid(self) -> Option<PgIntegerArrayOid> {
        match self {
            Self::Int2 => Some(PgIntegerArrayOid::Int2),
            Self::Int4 => Some(PgIntegerArrayOid::Int4),
            Self::Int8 => Some(PgIntegerArrayOid::Int8),
            _ => None,
        }
    }

    /// Maps a PostgreSQL integer array OID to its element type.
    #[must_use]
    pub fn from_integer_array_oid(oid: u32) -> Option<Self> {
        match PgIntegerArrayOid::from_oid(oid)? {
            PgIntegerArrayOid::Int2 => Some(Self::Int2),
            PgIntegerArrayOid::Int4 => Some(Self::Int4),
            PgIntegerArrayOid::Int8 => Some(Self::Int8),
        }
    }

    /// Formats an integer datum as SQL literal text.
    #[must_use]
    pub fn integer_sql_literal(self, value: i64) -> Option<String> {
        match self {
            Self::Int2 => i16::try_from(value).ok().map(|value| value.to_string()),
            Self::Int4 => i32::try_from(value).ok().map(|value| value.to_string()),
            Self::Int8 => Some(value.to_string()),
            _ => None,
        }
    }
}

impl PgIntegerArrayOid {
    /// Maps a PostgreSQL array type OID to the supported enum.
    #[must_use]
    pub const fn from_oid(oid: u32) -> Option<Self> {
        match oid {
            1005 => Some(Self::Int2),
            1007 => Some(Self::Int4),
            1016 => Some(Self::Int8),
            _ => None,
        }
    }

    /// Returns the PostgreSQL array type OID.
    #[must_use]
    pub const fn oid(self) -> u32 {
        self as u32
    }

    /// Returns the element type OID accepted by `deconstruct_array`.
    #[must_use]
    pub const fn element_oid(self) -> u32 {
        match self {
            Self::Int2 => 21,
            Self::Int4 => 23,
            Self::Int8 => 20,
        }
    }

    /// Returns the element `PgType`.
    #[must_use]
    pub const fn element_type(self) -> PgType {
        match self {
            Self::Int2 => PgType::Int2,
            Self::Int4 => PgType::Int4,
            Self::Int8 => PgType::Int8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PgType;
    use serde_json::json;

    #[test]
    fn pg_type_round_trips_through_json_alias_names() {
        let value = json!("bigint");
        let pg_type: PgType = serde_json::from_value(value).unwrap();
        assert_eq!(pg_type, PgType::Int8);
        assert_eq!(serde_json::to_value(pg_type).unwrap(), json!("int8"));
    }

    #[test]
    fn pg_type_discriminant_round_trip() {
        for pg_type in [
            PgType::Bool,
            PgType::Int2,
            PgType::Int4,
            PgType::Int8,
            PgType::Float4,
            PgType::Float8,
            PgType::Text,
            PgType::Numeric,
            PgType::Uuid,
            PgType::Jsonb,
            PgType::TextArray,
            PgType::Bytea,
            PgType::Timestamptz,
        ] {
            assert_eq!(
                PgType::from_discriminant(pg_type.discriminant()),
                Some(pg_type)
            );
        }
    }
}
