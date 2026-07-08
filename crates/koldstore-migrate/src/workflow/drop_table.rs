//! DROP TABLE cleanup planning for managed tables.

use koldstore_common::SqlStatement;
use thiserror::Error;

use crate::rehydrate::plan_catalog_deactivation;
use crate::QualifiedTableName;

/// DROP TABLE cleanup policies for object artifact handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropTableCleanupPolicy {
    /// Retain object artifact files and only deactivate local metadata.
    Retain,
    /// Delete object artifact files after catalog cleanup succeeds.
    Delete,
    /// Failed cleanup leaves jobs and metadata for operator recovery.
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
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
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
    pub statements: Vec<SqlStatement>,
}

/// Plans local metadata and object artifact cleanup for a managed DROP TABLE.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_drop_table_cleanup(
    table: QualifiedTableName,
    table_oid: u32,
    policy: DropTableCleanupPolicy,
) -> Result<DropTableCleanupPlan, DropTableCleanupError> {
    let deactivate = plan_catalog_deactivation(table_oid)
        .map_err(|error| DropTableCleanupError::Sql(error.to_string()))?;
    let mut statements = vec![deactivate];

    let outcome = match policy {
        DropTableCleanupPolicy::Retain => DropTableCleanupOutcome::MetadataDeactivated,
        DropTableCleanupPolicy::Delete => {
            statements.push(plan_drop_table_cleanup_job(table_oid, "pending", 0, None)?);
            DropTableCleanupOutcome::DeleteArtifactsQueued
        }
        DropTableCleanupPolicy::Failed => {
            statements.push(plan_drop_table_cleanup_job(
                table_oid,
                "error",
                1,
                Some("$2::text"),
            )?);
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

fn plan_drop_table_cleanup_job(
    table_oid: u32,
    status: &str,
    attempts: u32,
    error_trace_param: Option<&str>,
) -> Result<SqlStatement, DropTableCleanupError> {
    let _ = table_oid;
    let error_trace = error_trace_param.unwrap_or("NULL");
    SqlStatement::write(
        "drop table queue artifact cleanup",
        &format!(
            "INSERT INTO koldstore.jobs (id, table_oid, job_type, status, attempts, error_trace) VALUES (gen_random_uuid(), $1, 'drop_table_cleanup', '{status}', {attempts}, {error_trace})"
        ),
    )
    .map_err(|error| DropTableCleanupError::Sql(error.to_string()))
}
