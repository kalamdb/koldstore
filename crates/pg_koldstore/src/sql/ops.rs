//! Operational SQL helpers.

use koldstore_core::{ScopeKey, TableName};
use thiserror::Error;

use crate::spi::SpiStatement;

/// Placeholder status key names returned by table status.
pub const TABLE_STATUS_FIELDS: &[&str] = &[
    "hot_rows",
    "cold_segment_count",
    "manifest_state",
    "pending_jobs",
    "storage_binding",
    "last_error",
];

/// SQL-callable flush API function names exposed through pgrx.
pub const FLUSH_SQL_FUNCTIONS: &[&str] = &[
    "koldstore.set_flush_policy",
    "koldstore.flush_table",
    "koldstore.flush_pending",
];

/// Operational maintenance command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpsCommand {
    /// Export a managed table as a kalamdb-compatible archive.
    ExportTable { table_name: TableName },
    /// Import is a parser boundary until cold artifact ownership is implemented.
    ImportTable { table_name: TableName },
}

/// Operational planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OpsError {
    /// Unsupported command boundary.
    #[error("unsupported koldstore_exec command")]
    UnsupportedCommand,
    /// Import is intentionally not implemented in the MVP.
    #[error("IMPORT TABLE is not supported in this MVP")]
    ImportUnsupported,
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Planned table status query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStatusPlan {
    /// Table filter.
    pub table_name: TableName,
    /// Optional scope filter.
    pub scope_key: Option<ScopeKey>,
    /// Parameterized catalog statement.
    pub statement: SpiStatement,
}

/// Planned manifest backup query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifestPlan {
    /// Optional table filter.
    pub table_name: Option<TableName>,
    /// Optional scope filter.
    pub scope_key: Option<ScopeKey>,
    /// Parameterized manifest statement.
    pub statement: SpiStatement,
}

/// Planned cold storage validation query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateColdStoragePlan {
    /// Optional table filter.
    pub table_name: Option<TableName>,
    /// Parameterized validation seed statement.
    pub statement: SpiStatement,
}

/// Planned recovery query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverSegmentsPlan {
    /// Recovery request.
    pub request: RecoverSegmentsRequest,
    /// Parameterized recovery/job statement.
    pub statement: SpiStatement,
}

/// Planned `koldstore_exec` export/import boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KoldstoreExecPlan {
    /// Parsed command.
    pub command: OpsCommand,
    /// Archive manifest path for export commands.
    pub archive_manifest_path: String,
    /// Parameterized export statement.
    pub statement: SpiStatement,
}

/// Result of a cold-storage validation run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidationSummary {
    /// Number of manifest records checked.
    pub manifests_checked: u64,
    /// Number of cold segments checked.
    pub segments_checked: u64,
    /// Whether catalog consistency checks passed.
    pub catalog_consistent: bool,
}

/// Recovery request for orphan objects and local catalog repair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverSegmentsRequest {
    /// Optional table filter.
    pub table_name: Option<TableName>,
    /// Dry-run mode records what would happen without mutating cold artifacts.
    pub dry_run: bool,
}

/// Flush request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushRequest {
    /// Table name.
    pub table_name: TableName,
    /// Optional scope key.
    pub scope_key: Option<ScopeKey>,
    /// Force flush.
    pub force: bool,
}

/// Flush policy update request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetFlushPolicyRequest {
    /// Table name.
    pub table_name: TableName,
    /// New flush policy, or `None` to disable automatic flush.
    pub flush_policy: Option<String>,
}

/// Flush-pending request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushPendingRequest {
    /// Maximum pending scopes to flush.
    pub limit: u32,
}

/// Creates a flush policy update request.
#[must_use]
pub const fn set_flush_policy_request(
    table_name: TableName,
    flush_policy: Option<String>,
) -> SetFlushPolicyRequest {
    SetFlushPolicyRequest {
        table_name,
        flush_policy,
    }
}

/// Creates a flush request.
#[must_use]
pub const fn flush_table_request(
    table_name: TableName,
    scope_key: Option<ScopeKey>,
    force: bool,
) -> FlushRequest {
    FlushRequest {
        table_name,
        scope_key,
        force,
    }
}

/// Creates a flush-pending request.
#[must_use]
pub const fn flush_pending_request(limit: u32) -> FlushPendingRequest {
    FlushPendingRequest { limit }
}

/// Parses the limited `koldstore_exec` command boundary.
#[must_use]
pub fn classify_command(command: &str) -> Option<OpsCommand> {
    let normalized = command.trim();
    let upper = normalized.to_ascii_uppercase();
    if upper.starts_with("EXPORT TABLE ") {
        TableName::parse(&normalized["EXPORT TABLE ".len()..])
            .ok()
            .map(|table_name| OpsCommand::ExportTable { table_name })
    } else if upper.starts_with("IMPORT TABLE ") {
        TableName::parse(&normalized["IMPORT TABLE ".len()..])
            .ok()
            .map(|table_name| OpsCommand::ImportTable { table_name })
    } else {
        None
    }
}

/// Plans `koldstore.table_status`.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn table_status_plan(
    table_name: TableName,
    scope_key: Option<ScopeKey>,
) -> Result<TableStatusPlan, OpsError> {
    let statement = SpiStatement::read(
        "table status",
        "SELECT s.table_oid, m.sync_state AS manifest_state, COALESCE(m.segment_count, 0) AS cold_segment_count, COALESCE(j.pending_jobs, 0) AS pending_jobs, s.storage_id AS storage_binding, m.last_error FROM system.schemas s LEFT JOIN koldstore.manifest m ON m.table_oid = s.table_oid LEFT JOIN LATERAL (SELECT count(*) AS pending_jobs FROM system.jobs j WHERE j.table_oid = s.table_oid AND j.status IN ('pending', 'running')) j ON true WHERE s.table_oid = $1::regclass::oid AND ($2::text IS NULL OR m.scope_key = $2)",
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(TableStatusPlan {
        table_name,
        scope_key,
        statement,
    })
}

/// Plans `koldstore.backup_manifest`.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn backup_manifest_plan(
    table_name: Option<TableName>,
    scope_key: Option<ScopeKey>,
) -> Result<BackupManifestPlan, OpsError> {
    let statement = SpiStatement::read(
        "backup manifest",
        "SELECT manifest_path, etag, generation, max_seq, max_commit_seq FROM koldstore.manifest WHERE ($1::regclass IS NULL OR table_oid = $1::regclass::oid) AND ($2::text IS NULL OR scope_key = $2)",
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(BackupManifestPlan {
        table_name,
        scope_key,
        statement,
    })
}

/// Plans `koldstore.validate_cold_storage`.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn validate_cold_storage_plan(
    table_name: Option<TableName>,
) -> Result<ValidateColdStoragePlan, OpsError> {
    let statement = SpiStatement::read(
        "validate cold storage",
        "SELECT m.manifest_path, cs.object_path, cs.row_count, cs.column_stats, h.pk_hash FROM koldstore.manifest m LEFT JOIN koldstore.cold_segments cs ON cs.table_oid = m.table_oid LEFT JOIN koldstore.cold_pk_hints h ON h.table_oid = cs.table_oid WHERE ($1::regclass IS NULL OR m.table_oid = $1::regclass::oid)",
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(ValidateColdStoragePlan {
        table_name,
        statement,
    })
}

/// Plans `koldstore.recover_segments`.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn recover_segments_plan(
    table_name: Option<TableName>,
    dry_run: bool,
) -> Result<RecoverSegmentsPlan, OpsError> {
    let statement = SpiStatement::write(
        "recover segments",
        "INSERT INTO system.jobs (id, table_oid, job_type, status, attempts, error_trace) SELECT gen_random_uuid(), $1::regclass::oid, 'recover_segments', CASE WHEN $2::boolean THEN 'dry_run' ELSE 'pending' END, 0, NULL RETURNING id",
    )
    .map_err(|error| OpsError::Spi(error.to_string()))?;

    Ok(RecoverSegmentsPlan {
        request: RecoverSegmentsRequest {
            table_name,
            dry_run,
        },
        statement,
    })
}

/// Plans the limited `koldstore_exec` export/import boundary.
///
/// # Errors
///
/// Returns an error for unsupported commands, unsupported imports, or invalid
/// SPI statement metadata.
pub fn plan_koldstore_exec(command: &str) -> Result<KoldstoreExecPlan, OpsError> {
    match classify_command(command).ok_or(OpsError::UnsupportedCommand)? {
        OpsCommand::ExportTable { table_name } => {
            let archive_manifest_path =
                format!("{}/manifest.json", table_name.as_str().replace('.', "/"));
            let statement = SpiStatement::read(
                "export table archive",
                "SELECT m.manifest_path, cs.object_path, cs.row_count, cs.byte_size FROM koldstore.manifest m LEFT JOIN koldstore.cold_segments cs ON cs.table_oid = m.table_oid WHERE m.table_oid = $1::regclass::oid",
            )
            .map_err(|error| OpsError::Spi(error.to_string()))?;
            Ok(KoldstoreExecPlan {
                command: OpsCommand::ExportTable { table_name },
                archive_manifest_path,
                statement,
            })
        }
        OpsCommand::ImportTable { .. } => Err(OpsError::ImportUnsupported),
    }
}
