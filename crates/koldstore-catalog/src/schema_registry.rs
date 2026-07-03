//! Schema registry models.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use koldstore_core::{Diagnostic, KoldstoreError, Result};

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

    /// Creates a system column description.
    #[must_use]
    pub fn system(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable: false,
            system: true,
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
    /// Validates required schema registry invariants.
    ///
    /// # Errors
    ///
    /// Returns catalog diagnostics for missing primary key or system columns.
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

        for required in ["_seq", "_commit_seq", "_deleted"] {
            if !self
                .columns
                .iter()
                .any(|column| column.name == required && column.system)
            {
                return Err(KoldstoreError::CatalogValidation {
                    diagnostic: Diagnostic::new(
                        "missing_system_column",
                        format!("required system column not present: {required}"),
                    ),
                });
            }
        }

        Ok(())
    }
}
