//! Existing-row backfill helpers.

use thiserror::Error;

use crate::spi::SpiStatement;
use crate::sql::session;

use super::lock::MigrationLockKey;
use super::QualifiedTableName;

/// Backfill batch sizing default.
pub const DEFAULT_BACKFILL_BATCH_ROWS: usize = 10_000;

/// Existing-row backfill planning result.
pub type BackfillResult<T> = Result<T, BackfillError>;

/// Existing-row backfill planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BackfillError {
    /// Table oid is missing.
    #[error("table_oid cannot be zero")]
    MissingTableOid,
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Existing-row backfill SQL expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillPlan {
    /// Relation name.
    pub table_name: String,
}

impl BackfillPlan {
    /// Creates a backfill plan.
    #[must_use]
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }

    /// SQL statement that fills missing system columns.
    #[must_use]
    pub fn sql(&self) -> String {
        format!(
            "UPDATE {} SET _seq = COALESCE(_seq, SNOWFLAKE_ID()), _commit_seq = COALESCE(_commit_seq, nextval('koldstore.global_commit_seq'::regclass)), _deleted = COALESCE(_deleted, false) WHERE _seq IS NULL OR _commit_seq IS NULL OR _deleted IS NULL",
            self.table_name
        )
    }
}

/// Planned existing-row backfill under a migration advisory lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingRowBackfillPlan {
    /// Target table oid.
    pub table_oid: u32,
    /// Advisory lock key to bind into the first statement.
    pub lock_key: MigrationLockKey,
    /// DDL/DML statements to execute in order.
    pub statements: Vec<SpiStatement>,
}

/// Builds a safe existing-row backfill plan for a qualified table.
///
/// # Errors
///
/// Returns an error when `table_oid` is zero or statement metadata cannot be
/// prepared.
pub fn plan_existing_row_backfill(
    table: &QualifiedTableName,
    table_oid: u32,
) -> BackfillResult<ExistingRowBackfillPlan> {
    if table_oid == 0 {
        return Err(BackfillError::MissingTableOid);
    }

    let lock_key = MigrationLockKey::for_table(table_oid);
    let statements = [
        "SELECT pg_advisory_xact_lock($1, $2)".to_string(),
        format!(
            "UPDATE ONLY {} SET \
             \"_seq\" = COALESCE(\"_seq\", {}), \
             \"_commit_seq\" = COALESCE(\"_commit_seq\", nextval('koldstore.global_commit_seq'::regclass)), \
             \"_deleted\" = COALESCE(\"_deleted\", false) \
             WHERE \"_seq\" IS NULL OR \"_commit_seq\" IS NULL OR \"_deleted\" IS NULL",
            table.quoted(),
            session::snowflake_default_expression()
        ),
    ]
    .into_iter()
    .map(|sql| SpiStatement::write("backfill existing rows", &sql))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|error| BackfillError::Spi(error.to_string()))?;

    Ok(ExistingRowBackfillPlan {
        table_oid,
        lock_key,
        statements,
    })
}
