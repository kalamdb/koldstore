//! Migration rollback cleanup helpers.

use thiserror::Error;

use crate::spi::SpiStatement;

use super::QualifiedTableName;

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
    /// Clean-schema mirror table to drop if it was created before failure.
    pub mirror_table: Option<QualifiedTableName>,
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
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            quoted_table_name: None,
            table_oid: None,
            mirror_table: None,
        }
    }

    /// Creates cleanup for a parsed relation and catalog table oid.
    #[must_use]
    pub fn for_table(table: QualifiedTableName, table_oid: u32) -> Self {
        Self {
            table_name: table.schema.as_ref().map_or_else(
                || table.name.clone(),
                |schema| format!("{schema}.{}", table.name),
            ),
            quoted_table_name: Some(table.quoted()),
            table_oid: Some(table_oid),
            mirror_table: None,
        }
    }

    /// Adds the mirror table that should be removed on rollback.
    #[must_use]
    pub fn with_mirror_table(mut self, mirror_table: QualifiedTableName) -> Self {
        self.mirror_table = Some(mirror_table);
        self
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

        if let Some(mirror_table) = &self.mirror_table {
            statements.push(
                SpiStatement::write(
                    "cleanup change-log mirror table",
                    &format!("DROP TABLE IF EXISTS {}", mirror_table.quoted()),
                )
                .map_err(|error| RollbackError::Spi(error.to_string()))?,
            );
        }

        for sql in [
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

        Ok(RollbackCleanupPlan {
            table_oid,
            statements,
        })
    }
}
