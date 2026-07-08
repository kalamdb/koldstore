//! DDL event-trigger integration.

pub use koldstore_migrate::drop_table::{
    plan_drop_table_cleanup, DropTableCleanupError, DropTableCleanupOutcome, DropTableCleanupPlan,
    DropTableCleanupPolicy,
};

/// Placeholder for managed-table DDL change detection.
pub fn handle_ddl_event() {
    // DROP TABLE handling records whether cold object artifact cleanup should be
    // retain, delete, or failed for later operator recovery.
}
