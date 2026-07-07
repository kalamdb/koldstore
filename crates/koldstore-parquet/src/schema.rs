//! PostgreSQL-to-Arrow schema model.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use arrow_schema::{DataType, Field, Schema, TimeUnit};

/// Supported PostgreSQL type class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgType {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Text,
    Numeric,
    Uuid,
    Jsonb,
    TextArray,
    Bytea,
    Timestamptz,
}

/// PostgreSQL column description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgColumn {
    pub name: String,
    pub pg_type: PgType,
    pub nullable: bool,
}

impl PgColumn {
    /// Creates a PostgreSQL column description.
    #[must_use]
    pub fn new(name: impl Into<String>, pg_type: PgType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            pg_type,
            nullable,
        }
    }

    /// Creates a column from PostgreSQL catalog type text.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnsupportedType`] when the type is outside the
    /// pg-koldstore MVP Arrow/Parquet support matrix.
    pub fn from_catalog(
        name: impl Into<String>,
        type_name: &str,
        nullable: bool,
    ) -> Result<Self, SchemaError> {
        Ok(Self::new(
            name,
            PgType::from_postgres_name(type_name)?,
            nullable,
        ))
    }

    /// Converts this column to an Arrow field.
    #[must_use]
    pub fn to_arrow_field(&self) -> Field {
        Field::new(&self.name, self.pg_type.arrow_type(), self.nullable)
    }
}

impl PgType {
    /// Parses PostgreSQL catalog type names and common aliases.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnsupportedType`] for unsupported or blank types.
    pub fn from_postgres_name(type_name: &str) -> Result<Self, SchemaError> {
        let normalized = canonical_type_name(type_name);
        match normalized.as_str() {
            "bool" | "boolean" => Ok(Self::Bool),
            "int2" | "smallint" => Ok(Self::Int2),
            "int4" | "integer" => Ok(Self::Int4),
            "int8" | "bigint" => Ok(Self::Int8),
            "float4" | "real" => Ok(Self::Float4),
            "float8" | "double precision" => Ok(Self::Float8),
            "text" | "varchar" | "character varying" => Ok(Self::Text),
            "numeric" => Ok(Self::Numeric),
            "uuid" => Ok(Self::Uuid),
            "jsonb" => Ok(Self::Jsonb),
            "text[]" => Ok(Self::TextArray),
            "bytea" => Ok(Self::Bytea),
            "timestamptz" | "timestamp with time zone" => Ok(Self::Timestamptz),
            _ => Err(SchemaError::UnsupportedType(normalized)),
        }
    }

    /// Returns the Arrow type for this supported PostgreSQL type.
    #[must_use]
    pub const fn arrow_type(&self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Int2 => DataType::Int16,
            Self::Int4 => DataType::Int32,
            Self::Int8 => DataType::Int64,
            Self::Float4 => DataType::Float32,
            Self::Float8 => DataType::Float64,
            Self::Text
            | Self::Numeric
            | Self::Uuid
            | Self::Jsonb
            | Self::TextArray
            | Self::Bytea => DataType::Utf8,
            Self::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }
}

/// Clean-schema cold metadata column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdMetadataColumn {
    Seq,
    Op,
    ChangedAt,
    Deleted,
    SchemaVersion,
}

impl ColdMetadataColumn {
    /// Returns the clean cold metadata column name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Seq => "seq",
            Self::Op => "op",
            Self::ChangedAt => "changed_at",
            Self::Deleted => "deleted",
            Self::SchemaVersion => "schema_version",
        }
    }

    /// Returns the Arrow field for this clean cold metadata column.
    #[must_use]
    pub fn field(self) -> Field {
        let data_type = match self {
            Self::Seq => DataType::Int64,
            Self::Op => DataType::Int16,
            Self::ChangedAt => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::Deleted => DataType::Boolean,
            Self::SchemaVersion => DataType::UInt32,
        };
        Field::new(self.name(), data_type, false)
    }
}

/// Schema conversion error.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    #[error("unsupported PostgreSQL type: {0}")]
    UnsupportedType(String),
}

/// Builds a clean-schema Arrow schema with mirror/cold metadata appended.
///
/// # Errors
///
/// Currently returns only future schema errors; all [`PgType`] variants are supported.
pub fn build_clean_arrow_schema(columns: &[PgColumn]) -> Result<Schema, SchemaError> {
    let mut fields: Vec<Field> = columns.iter().map(PgColumn::to_arrow_field).collect();
    fields.extend([
        ColdMetadataColumn::Seq.field(),
        ColdMetadataColumn::Op.field(),
        ColdMetadataColumn::ChangedAt.field(),
        ColdMetadataColumn::Deleted.field(),
        ColdMetadataColumn::SchemaVersion.field(),
    ]);
    Ok(Schema::new(fields))
}

fn normalize_type_name(type_name: &str) -> String {
    type_name
        .trim()
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn canonical_type_name(type_name: &str) -> String {
    let normalized = normalize_type_name(type_name);
    if normalized == "timestamp with time zone" {
        return normalized;
    }
    if normalized.starts_with("timestamp(") && normalized.ends_with(" with time zone") {
        return "timestamp with time zone".to_string();
    }
    if let Some((prefix, suffix)) = normalized.split_once('(') {
        if suffix.ends_with(')') {
            return prefix.trim().to_string();
        }
    }
    normalized
}
