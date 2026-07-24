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
    /// Object deletion should run after metadata cleanup (inline in the DROP hook).
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
    /// Catalog statements (`$1` = table oid). Object GC is not SQL.
    pub statements: Vec<SqlStatement>,
    /// Optional audit job row to insert after object GC (`$1` = table oid).
    pub audit_job: Option<SqlStatement>,
}

/// Plans local metadata and object artifact cleanup for a managed DROP TABLE.
///
/// Cancel of active jobs is owned by the extension shell (`plan_cancel_jobs_for_drop`)
/// so migrate stays free of flush-crate dependencies. Object-store GC for
/// [`DropTableCleanupPolicy::Delete`] runs inline in the ProcessUtility hook
/// (no background claimer yet); [`DropTableCleanupPlan::audit_job`] records
/// completion afterward.
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
    let statements = vec![
        deactivate,
        plan_delete_cold_segments()?,
        plan_delete_manifest()?,
    ];

    let (outcome, audit_job) = match policy {
        DropTableCleanupPolicy::Retain => (DropTableCleanupOutcome::MetadataDeactivated, None),
        DropTableCleanupPolicy::Delete => (
            DropTableCleanupOutcome::DeleteArtifactsQueued,
            Some(plan_drop_table_cleanup_job(
                DropTableCleanupPolicy::Delete,
                "completed",
                0,
                None,
            )?),
        ),
        DropTableCleanupPolicy::Failed => (
            DropTableCleanupOutcome::RecoveryRequired,
            Some(plan_drop_table_cleanup_job(
                DropTableCleanupPolicy::Failed,
                "error",
                1,
                Some("$2::text"),
            )?),
        ),
    };

    Ok(DropTableCleanupPlan {
        table,
        table_oid,
        policy,
        outcome,
        statements,
        audit_job,
    })
}

fn plan_delete_cold_segments() -> Result<SqlStatement, DropTableCleanupError> {
    SqlStatement::write(
        "drop table delete cold segments",
        "DELETE FROM koldstore.cold_segments WHERE table_oid = $1::oid",
    )
    .map_err(|error| DropTableCleanupError::Sql(error.to_string()))
}

fn plan_delete_manifest() -> Result<SqlStatement, DropTableCleanupError> {
    SqlStatement::write(
        "drop table delete manifest",
        "DELETE FROM koldstore.manifest WHERE table_oid = $1::oid",
    )
    .map_err(|error| DropTableCleanupError::Sql(error.to_string()))
}

fn plan_drop_table_cleanup_job(
    policy: DropTableCleanupPolicy,
    status: &str,
    attempts: u32,
    error_trace_param: Option<&str>,
) -> Result<SqlStatement, DropTableCleanupError> {
    let error_trace = error_trace_param.unwrap_or("NULL");
    let phase = match status {
        "completed" => "finished",
        "error" => "failed",
        _ => "pending",
    };
    let policy_label = match policy {
        DropTableCleanupPolicy::Retain => "retain",
        DropTableCleanupPolicy::Delete => "delete",
        DropTableCleanupPolicy::Failed => "failed",
    };
    SqlStatement::write(
        "drop table queue artifact cleanup",
        &format!(
            r#"
INSERT INTO koldstore.jobs (
    id, table_oid, job_type, status, phase, attempts, error_trace, payload
) VALUES (
    gen_random_uuid(),
    $1::oid,
    'drop_table_cleanup',
    '{status}',
    '{phase}',
    {attempts},
    {error_trace},
    jsonb_build_object('policy', '{policy_label}')
)
"#
        ),
    )
    .map_err(|error| DropTableCleanupError::Sql(error.to_string()))
}
