//! Operational SQL helpers.

/// Placeholder status key names returned by table status.
pub const TABLE_STATUS_FIELDS: &[&str] = &[
    "hot_rows",
    "cold_segment_count",
    "manifest_state",
    "pending_jobs",
    "storage_binding",
    "last_error",
];

/// Operational maintenance command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpsCommand {
    /// Export a managed table as a kalamdb-compatible archive.
    ExportTable { table_name: String },
    /// Import is a parser boundary until cold artifact ownership is implemented.
    ImportTable { table_name: String },
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
    pub table_name: Option<String>,
    /// Dry-run mode records what would happen without mutating cold artifacts.
    pub dry_run: bool,
}

/// Flush request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushRequest {
    /// Table name.
    pub table_name: String,
    /// Optional scope key.
    pub scope_key: Option<String>,
    /// Force flush.
    pub force: bool,
}

/// Creates a flush request.
#[must_use]
pub fn flush_table_request(
    table_name: impl Into<String>,
    scope_key: Option<String>,
    force: bool,
) -> FlushRequest {
    FlushRequest {
        table_name: table_name.into(),
        scope_key,
        force,
    }
}

/// Parses the limited `koldstore_exec` command boundary.
#[must_use]
pub fn classify_command(command: &str) -> Option<OpsCommand> {
    let normalized = command.trim();
    let upper = normalized.to_ascii_uppercase();
    if upper.starts_with("EXPORT TABLE ") {
        Some(OpsCommand::ExportTable {
            table_name: normalized["EXPORT TABLE ".len()..].trim().to_string(),
        })
    } else if upper.starts_with("IMPORT TABLE ") {
        Some(OpsCommand::ImportTable {
            table_name: normalized["IMPORT TABLE ".len()..].trim().to_string(),
        })
    } else {
        None
    }
}
