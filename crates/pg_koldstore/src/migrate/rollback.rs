//! Migration rollback cleanup helpers.

use thiserror::Error;

use crate::spi::SpiStatement;

use super::QualifiedTableName;

/// Rollback cleanup phases.
pub const ROLLBACK_PHASES: &[&str] = &["catalog_rows", "system_columns", "manifest_state"];

/// Rollback cleanup planning result.
pub type RollbackResult<T> = Result<T, RollbackError>;

/// Rollback cleanup planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RollbackError {
    /// Table oid is missing.
    #[error("table_oid cannot be zero")]
    MissingTableOid,
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Rollback cleanup plan for failed migrations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackCleanup {
    /// Relation name.
    pub table_name: String,
    /// Safely quoted relation name.
    pub quoted_table_name: Option<String>,
    /// Target relation oid.
    pub table_oid: Option<u32>,
    /// System columns to remove if the transaction is not already aborting.
    pub system_columns: Vec<String>,
}

/// Planned rollback cleanup statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackCleanupPlan {
    /// Target table oid.
    pub table_oid: u32,
    /// Cleanup statements in dependency order.
    pub statements: Vec<SpiStatement>,
}

impl RollbackCleanup {
    /// Creates cleanup for a relation.
    #[must_use]
    pub fn new(table_name: impl Into<String>, system_columns: Vec<String>) -> Self {
        Self {
            table_name: table_name.into(),
            quoted_table_name: None,
            table_oid: None,
            system_columns,
        }
    }

    /// Creates cleanup for a parsed relation and catalog table oid.
    #[must_use]
    pub fn for_table(
        table: QualifiedTableName,
        table_oid: u32,
        system_columns: Vec<String>,
    ) -> Self {
        Self {
            table_name: table.schema.as_ref().map_or_else(
                || table.name.clone(),
                |schema| format!("{schema}.{}", table.name),
            ),
            quoted_table_name: Some(table.quoted()),
            table_oid: Some(table_oid),
            system_columns,
        }
    }

    /// Builds cleanup statements for partial migration artifacts.
    ///
    /// # Errors
    ///
    /// Returns an error when the target oid is missing or a statement cannot be
    /// represented by the SPI helper boundary.
    pub fn plan(&self) -> RollbackResult<RollbackCleanupPlan> {
        let table_oid = self
            .table_oid
            .filter(|oid| *oid != 0)
            .ok_or(RollbackError::MissingTableOid)?;
        let mut statements = Vec::with_capacity(6);

        for sql in [
            "DELETE FROM koldstore.row_events WHERE table_oid = $1",
            "DELETE FROM koldstore.cold_pk_hints WHERE table_oid = $1",
            "DELETE FROM koldstore.cold_segments WHERE table_oid = $1",
            "DELETE FROM koldstore.manifest WHERE table_oid = $1",
            "DELETE FROM koldstore.schemas WHERE table_oid = $1",
        ] {
            statements.push(
                SpiStatement::write("cleanup migration catalog rows", sql)
                    .map_err(|error| RollbackError::Spi(error.to_string()))?,
            );
        }

        if let Some(drop_columns_sql) = self.drop_system_columns_sql() {
            statements.push(
                SpiStatement::write("cleanup migration system columns", &drop_columns_sql)
                    .map_err(|error| RollbackError::Spi(error.to_string()))?,
            );
        }

        Ok(RollbackCleanupPlan {
            table_oid,
            statements,
        })
    }

    fn drop_system_columns_sql(&self) -> Option<String> {
        let quoted_table_name = self.quoted_table_name.as_ref()?;
        let columns = self
            .system_columns
            .iter()
            .map(|column| column.trim())
            .filter(|column| is_safe_identifier(column))
            .map(|column| format!("DROP COLUMN IF EXISTS \"{column}\""))
            .collect::<Vec<_>>();

        if columns.is_empty() {
            None
        } else {
            Some(format!(
                "ALTER TABLE ONLY {quoted_table_name} {}",
                columns.join(", ")
            ))
        }
    }
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
