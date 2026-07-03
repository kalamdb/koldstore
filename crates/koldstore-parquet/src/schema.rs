//! PostgreSQL-to-Arrow schema model.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use arrow::datatypes::{DataType, Field, Schema};

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
    Uuid,
    Jsonb,
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

    /// Converts this column to an Arrow field.
    #[must_use]
    pub fn to_arrow_field(&self) -> Field {
        Field::new(&self.name, self.pg_type.arrow_type(), self.nullable)
    }
}

impl PgType {
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
            Self::Text | Self::Uuid | Self::Jsonb => DataType::Utf8,
            Self::Timestamptz => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        }
    }
}

/// Required system column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemColumn {
    Seq,
    CommitSeq,
    Deleted,
}

impl SystemColumn {
    /// Returns the system column name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Seq => "_seq",
            Self::CommitSeq => "_commit_seq",
            Self::Deleted => "_deleted",
        }
    }

    /// Returns the Arrow field for this system column.
    #[must_use]
    pub fn field(self) -> Field {
        let data_type = match self {
            Self::Deleted => DataType::Boolean,
            Self::Seq | Self::CommitSeq => DataType::Int64,
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

/// Builds an Arrow schema with required system columns appended.
///
/// # Errors
///
/// Currently returns only future schema errors; all [`PgType`] variants are supported.
pub fn build_arrow_schema(columns: &[PgColumn]) -> Result<Schema, SchemaError> {
    let mut fields: Vec<Field> = columns.iter().map(PgColumn::to_arrow_field).collect();
    fields.push(SystemColumn::Seq.field());
    fields.push(SystemColumn::CommitSeq.field());
    fields.push(SystemColumn::Deleted.field());
    Ok(Schema::new(fields))
}
