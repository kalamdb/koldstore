//! Schema registry models.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use koldstore_common::{ColumnId, Diagnostic, KoldstoreError};

use crate::{PgType, SchemaError};

/// Schema column with stable logical identity and a versioned display name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaColumn {
    /// Stable logical ID (`pg_attribute.attnum` for the managed source relation).
    pub column_id: ColumnId,
    /// Column name in this schema version.
    pub name: String,
    pub pg_type: PgType,
    /// Original catalog type spelling preserved for diagnostics and matrix capture.
    pub catalog_type_name: String,
    pub nullable: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct SchemaColumnWire {
    column_id: i16,
    name: String,
    type_name: String,
    nullable: bool,
}

impl TryFrom<SchemaColumnWire> for SchemaColumn {
    type Error = SchemaError;

    fn try_from(wire: SchemaColumnWire) -> std::result::Result<Self, Self::Error> {
        let column_id = ColumnId::new(wire.column_id).map_err(|err| {
            SchemaError::UnsupportedType(format!("invalid column_id in schema wire: {err}"))
        })?;
        Ok(Self {
            column_id,
            name: wire.name,
            pg_type: PgType::from_postgres_name(&wire.type_name)?,
            catalog_type_name: wire.type_name,
            nullable: wire.nullable,
        })
    }
}

impl Serialize for SchemaColumn {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SchemaColumnWire {
            column_id: self.column_id.get(),
            name: self.name.clone(),
            type_name: self.catalog_type_name.clone(),
            nullable: self.nullable,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SchemaColumn {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        SchemaColumnWire::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

impl SchemaColumn {
    /// Creates an application column from a supported PostgreSQL catalog type name.
    ///
    /// # Panics
    ///
    /// Panics when `type_name` is outside the MVP support matrix. Tests and
    /// builders should prefer [`Self::typed`].
    #[must_use]
    pub fn app(
        column_id: i16,
        name: impl Into<String>,
        type_name: impl Into<String>,
        nullable: bool,
    ) -> Self {
        let catalog_type_name = type_name.into();
        let pg_type = PgType::from_postgres_name(&catalog_type_name)
            .expect("schema column builders must use supported PostgreSQL types");
        Self::typed(column_id, name, pg_type, catalog_type_name, nullable)
    }

    /// Creates an application column from a supported PostgreSQL type.
    #[must_use]
    pub fn typed(
        column_id: i16,
        name: impl Into<String>,
        pg_type: PgType,
        catalog_type_name: impl Into<String>,
        nullable: bool,
    ) -> Self {
        Self {
            column_id: ColumnId::from_attnum(column_id),
            name: name.into(),
            pg_type,
            catalog_type_name: catalog_type_name.into(),
            nullable,
        }
    }

    /// Returns the original catalog type spelling.
    #[must_use]
    pub fn catalog_type_name(&self) -> &str {
        &self.catalog_type_name
    }

    /// Returns the stable column ID.
    #[must_use]
    pub fn column_id(&self) -> ColumnId {
        self.column_id
    }
}

/// Registry row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaRegistryEntry {
    pub id: Uuid,
    pub table_oid: u32,
    pub version: u32,
    pub columns: Vec<SchemaColumn>,
}

impl SchemaRegistryEntry {
    /// Returns all registered columns for this schema version.
    #[must_use]
    pub fn application_columns(&self) -> Vec<&SchemaColumn> {
        self.columns.iter().collect()
    }

    /// Looks up a column by stable ID.
    #[must_use]
    pub fn column_by_id(&self, column_id: ColumnId) -> Option<&SchemaColumn> {
        self.columns
            .iter()
            .find(|column| column.column_id == column_id)
    }

    /// Resolves the physical/display name for a column ID in this schema version.
    #[must_use]
    pub fn physical_name(&self, column_id: ColumnId) -> Option<&str> {
        self.column_by_id(column_id)
            .map(|column| column.name.as_str())
    }

    /// Validates required schema registry invariants.
    ///
    /// Primary-key identity is compared by stable column IDs.
    ///
    /// # Errors
    ///
    /// Returns catalog diagnostics for missing primary key or missing primary-key
    /// columns. Clean-schema entries do not require user-table system columns.
    pub fn validate(&self, primary_key: &[ColumnId]) -> koldstore_common::Result<()> {
        if primary_key.is_empty() {
            return Err(KoldstoreError::CatalogValidation {
                diagnostic: Diagnostic::new(
                    "missing_primary_key",
                    "managed tables require a primary key",
                ),
            });
        }

        for pk_column_id in primary_key {
            if self.column_by_id(*pk_column_id).is_none() {
                return Err(KoldstoreError::CatalogValidation {
                    diagnostic: Diagnostic::new(
                        "missing_primary_key_column",
                        format!("primary key column_id not present in schema: {pk_column_id}"),
                    ),
                });
            }
        }

        Ok(())
    }
}
