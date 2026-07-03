//! Migration lock helpers.

/// Advisory lock namespace for migration operations.
pub const MIGRATION_LOCK_NAMESPACE: i64 = 0x6b6f6c6473746f;

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
