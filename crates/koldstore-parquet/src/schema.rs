//! PostgreSQL-to-Arrow schema model.

use std::collections::HashMap;

use koldstore_common::ColumnId;
use koldstore_schema::{PgType, SchemaError};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use serde::{Deserialize, Serialize};

use arrow_schema::{Field, Schema};

use crate::pg_type_codec::arrow_data_type;

/// PostgreSQL column description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgColumn {
    /// Stable application-column identity written as the Parquet field id.
    pub column_id: ColumnId,
    pub name: String,
    pub pg_type: PgType,
    pub nullable: bool,
}

impl PgColumn {
    /// Creates a PostgreSQL column description.
    #[must_use]
    pub fn new(
        column_id: ColumnId,
        name: impl Into<String>,
        pg_type: PgType,
        nullable: bool,
    ) -> Self {
        Self {
            column_id,
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
        column_id: ColumnId,
        name: impl Into<String>,
        type_name: &str,
        nullable: bool,
    ) -> Result<Self, SchemaError> {
        Ok(Self::new(
            column_id,
            name,
            PgType::from_postgres_name(type_name)?,
            nullable,
        ))
    }

    /// Converts this column to an Arrow field.
    #[must_use]
    pub fn to_arrow_field(&self) -> Field {
        let metadata = HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            self.column_id.to_string(),
        )]);
        Field::new(&self.name, arrow_data_type(self.pg_type), self.nullable).with_metadata(metadata)
    }
}

/// Clean-schema cold metadata column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdMetadataColumn {
    Seq,
    Op,
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
            Self::Deleted => "deleted",
            Self::SchemaVersion => "schema_version",
        }
    }

    /// Returns the Arrow field for this clean cold metadata column.
    #[must_use]
    pub fn field(self) -> Field {
        use arrow_schema::DataType;

        let data_type = match self {
            Self::Seq => DataType::Int64,
            Self::Op => DataType::Int16,
            Self::Deleted => DataType::Boolean,
            Self::SchemaVersion => DataType::UInt32,
        };
        Field::new(self.name(), data_type, false)
    }
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
        ColdMetadataColumn::Deleted.field(),
        ColdMetadataColumn::SchemaVersion.field(),
    ]);
    Ok(Schema::new(fields))
}
