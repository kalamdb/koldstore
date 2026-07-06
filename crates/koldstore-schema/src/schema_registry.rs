//! Schema registry models.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use koldstore_common::{Diagnostic, KoldstoreError, Result};

/// Schema column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaColumn {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub system: bool,
}

impl SchemaColumn {
    /// Creates an application column description.
    #[must_use]
    pub fn app(name: impl Into<String>, type_name: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable,
            system: false,
        }
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
    pub fn validate(&self, primary_key: &[&str]) -> Result<()> {
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
