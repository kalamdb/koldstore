//! Schema registry models.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use koldstore_common::{Diagnostic, KoldstoreError};

use crate::{PgType, SchemaError};

/// Schema column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaColumn {
    pub name: String,
    pub pg_type: PgType,
    /// Original catalog type spelling preserved for diagnostics and matrix capture.
    pub catalog_type_name: String,
    pub nullable: bool,
    pub system: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct SchemaColumnWire {
    name: String,
    type_name: String,
    nullable: bool,
    #[serde(default)]
    system: bool,
}

impl TryFrom<SchemaColumnWire> for SchemaColumn {
    type Error = SchemaError;

    fn try_from(wire: SchemaColumnWire) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            name: wire.name,
            pg_type: PgType::from_postgres_name(&wire.type_name)?,
            catalog_type_name: wire.type_name,
            nullable: wire.nullable,
            system: wire.system,
        })
    }
}

impl Serialize for SchemaColumn {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SchemaColumnWire {
            name: self.name.clone(),
            type_name: self.catalog_type_name.clone(),
            nullable: self.nullable,
            system: self.system,
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
    pub fn app(name: impl Into<String>, type_name: impl Into<String>, nullable: bool) -> Self {
        let catalog_type_name = type_name.into();
        let pg_type = PgType::from_postgres_name(&catalog_type_name)
            .expect("schema column builders must use supported PostgreSQL types");
        Self::typed(name, pg_type, catalog_type_name, nullable, false)
    }

    /// Creates an application column from a supported PostgreSQL type.
    #[must_use]
    pub fn typed(
        name: impl Into<String>,
        pg_type: PgType,
        catalog_type_name: impl Into<String>,
        nullable: bool,
        system: bool,
    ) -> Self {
        Self {
            name: name.into(),
            pg_type,
            catalog_type_name: catalog_type_name.into(),
            nullable,
            system,
        }
    }

    /// Returns the original catalog type spelling.
    #[must_use]
    pub fn catalog_type_name(&self) -> &str {
        &self.catalog_type_name
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
    /// Returns application-owned columns, excluding KoldStore metadata.
    #[must_use]
    pub fn application_columns(&self) -> Vec<&SchemaColumn> {
        self.columns
            .iter()
            .filter(|column| !column.system)
            .collect()
    }

    /// Returns KoldStore-owned metadata columns if a legacy schema still has any.
    #[must_use]
    pub fn system_columns(&self) -> Vec<&SchemaColumn> {
        self.columns.iter().filter(|column| column.system).collect()
    }

    /// Validates required schema registry invariants.
    ///
    /// # Errors
    ///
    /// Returns catalog diagnostics for missing primary key or missing primary-key
    /// columns. Clean-schema entries do not require user-table system columns.
    pub fn validate(&self, primary_key: &[&str]) -> koldstore_common::Result<()> {
        if primary_key.is_empty() {
            return Err(KoldstoreError::CatalogValidation {
                diagnostic: Diagnostic::new(
                    "missing_primary_key",
                    "managed tables require a primary key",
                ),
            });
        }

        for pk_column in primary_key {
            if !self.columns.iter().any(|column| column.name == *pk_column) {
                return Err(KoldstoreError::CatalogValidation {
                    diagnostic: Diagnostic::new(
                        "missing_primary_key_column",
                        format!("primary key column not present in schema: {pk_column}"),
                    ),
                });
            }
        }

        Ok(())
    }
}
