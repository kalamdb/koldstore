//! Migration rollback cleanup helpers.

/// Rollback cleanup phases.
pub const ROLLBACK_PHASES: &[&str] = &["catalog_rows", "system_columns", "manifest_state"];

/// Rollback cleanup plan for failed migrations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackCleanup {
    /// Relation name.
    pub table_name: String,
    /// System columns to remove if the transaction is not already aborting.
    pub system_columns: Vec<String>,
}

impl RollbackCleanup {
    /// Creates cleanup for a relation.
    #[must_use]
    pub fn new(table_name: impl Into<String>, system_columns: Vec<String>) -> Self {
        Self {
            table_name: table_name.into(),
            system_columns,
        }
    }
}
