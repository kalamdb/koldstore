//! CustomScan plan serialization.

/// Serialized custom-plan identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeScanPlan {
    /// Managed table oid.
    pub table_oid: u32,
    /// Logical primary-key columns.
    pub primary_key_columns: Vec<String>,
}

impl MergeScanPlan {
    /// Creates a merge scan plan.
    #[must_use]
    pub fn new(table_oid: u32, primary_key_columns: Vec<String>) -> Self {
        Self {
            table_oid,
            primary_key_columns,
        }
    }
}
