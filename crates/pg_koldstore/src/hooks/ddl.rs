//! DDL event-trigger integration.

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

/// Placeholder for managed-table DDL change detection.
pub fn handle_ddl_event() {
    // DROP TABLE handling records whether cold object artifact cleanup should be
    // retain, delete, or failed for later operator recovery.
}
