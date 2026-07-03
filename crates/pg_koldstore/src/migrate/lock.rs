//! Migration lock helpers.

use thiserror::Error;

use crate::spi::SpiStatement;

use super::QualifiedTableName;

/// Advisory lock namespace for migration operations.
pub const MIGRATION_LOCK_NAMESPACE: i64 = 0x6b6f6c6473746f;

/// Migration operation lock planning result.
pub type LockResult<T> = Result<T, LockError>;

/// Migration operation lock planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LockError {
    /// Table oid is missing.
    #[error("table_oid cannot be zero")]
    MissingTableOid,
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Migration lock key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationLockKey {
    /// Namespace.
    pub namespace: i64,
    /// Table oid.
    pub table_oid: u32,
}

impl MigrationLockKey {
    /// Builds a lock key for a table oid.
    #[must_use]
    pub const fn for_table(table_oid: u32) -> Self {
        Self {
            namespace: MIGRATION_LOCK_NAMESPACE,
            table_oid,
        }
    }
}

/// Planned locks for a table migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationOperationLockPlan {
    /// Target table oid.
    pub table_oid: u32,
    /// Advisory lock key to bind into the first statement.
    pub lock_key: MigrationLockKey,
    /// Lock statements in acquisition order.
    pub statements: Vec<SpiStatement>,
}

/// Builds lock statements that serialize migration and block concurrent DDL/DML.
///
/// # Errors
///
/// Returns an error when `table_oid` is zero or lock statement metadata cannot
/// be represented by the SPI helper boundary.
pub fn plan_migration_operation_lock(
    table: &QualifiedTableName,
    table_oid: u32,
) -> LockResult<MigrationOperationLockPlan> {
    if table_oid == 0 {
        return Err(LockError::MissingTableOid);
    }

    let lock_key = MigrationLockKey::for_table(table_oid);
    let statements = [
        "SELECT pg_advisory_xact_lock($1, $2)".to_string(),
        format!(
            "LOCK TABLE ONLY {} IN ACCESS EXCLUSIVE MODE",
            table.quoted()
        ),
    ]
    .into_iter()
    .map(|sql| SpiStatement::write("lock table for migration", &sql))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|error| LockError::Spi(error.to_string()))?;

    Ok(MigrationOperationLockPlan {
        table_oid,
        lock_key,
        statements,
    })
}
