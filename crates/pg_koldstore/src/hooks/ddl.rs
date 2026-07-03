//! DDL event-trigger integration.

use thiserror::Error;

use crate::migrate::QualifiedTableName;
use crate::spi::SpiStatement;

/// DROP TABLE cleanup policies for object artifact handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropTableCleanupPolicy {
    /// retain object artifact files and only deactivate local metadata.
    Retain,
    /// delete object artifact files after catalog cleanup succeeds.
    Delete,
    /// failed cleanup leaves jobs and metadata for operator recovery.
    Failed,
}

/// Outcome recorded for a DROP TABLE cleanup plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropTableCleanupOutcome {
    /// Local metadata is deactivated and object artifacts remain.
    MetadataDeactivated,
    /// Object deletion is queued after metadata cleanup.
    DeleteArtifactsQueued,
    /// Cleanup failed and operator recovery is required.
    RecoveryRequired,
}

/// DROP TABLE cleanup planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DropTableCleanupError {
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Planned DROP TABLE cleanup work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableCleanupPlan {
    /// Dropped table.
    pub table: QualifiedTableName,
    /// Dropped table oid.
    pub table_oid: u32,
    /// Cleanup policy.
    pub policy: DropTableCleanupPolicy,
    /// Cleanup outcome.
    pub outcome: DropTableCleanupOutcome,
    /// Parameterized statements to run.
    pub statements: Vec<SpiStatement>,
}

/// Plans local metadata and object artifact cleanup for a managed DROP TABLE.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_drop_table_cleanup(
    table: QualifiedTableName,
    table_oid: u32,
    policy: DropTableCleanupPolicy,
) -> Result<DropTableCleanupPlan, DropTableCleanupError> {
    let deactivate = SpiStatement::write(
        "drop table deactivate metadata",
        "UPDATE system.schemas SET active = false WHERE table_oid = $1",
    )
    .map_err(|error| DropTableCleanupError::Spi(error.to_string()))?;
    let mut statements = vec![deactivate];

    let outcome = match policy {
        DropTableCleanupPolicy::Retain => DropTableCleanupOutcome::MetadataDeactivated,
        DropTableCleanupPolicy::Delete => {
            statements.push(
                SpiStatement::write(
                    "drop table queue artifact cleanup",
                    "INSERT INTO system.jobs (id, table_oid, job_type, status, attempts, error_trace) VALUES (gen_random_uuid(), $1, 'drop_table_cleanup', 'pending', 0, NULL)",
                )
                .map_err(|error| DropTableCleanupError::Spi(error.to_string()))?,
            );
            DropTableCleanupOutcome::DeleteArtifactsQueued
        }
        DropTableCleanupPolicy::Failed => {
            statements.push(
                SpiStatement::write(
                    "drop table record cleanup failure",
                    "INSERT INTO system.jobs (id, table_oid, job_type, status, attempts, error_trace) VALUES (gen_random_uuid(), $1, 'drop_table_cleanup', 'error', 1, $2)",
                )
                .map_err(|error| DropTableCleanupError::Spi(error.to_string()))?,
            );
            DropTableCleanupOutcome::RecoveryRequired
        }
    };

    Ok(DropTableCleanupPlan {
        table,
        table_oid,
        policy,
        outcome,
        statements,
    })
}

/// Placeholder for managed-table DDL change detection.
pub fn handle_ddl_event() {
    // DROP TABLE handling records whether cold object artifact cleanup should be
    // retain, delete, or failed for later operator recovery.
}
