//! Schema registry insertion helpers.

/// Initial schema version for a managed table.
pub const INITIAL_SCHEMA_VERSION: u32 = 1;

/// Metadata recorded for a greenfield registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationMetadata {
    /// Table oid.
    pub table_oid: u32,
    /// Table type.
    pub table_type: String,
    /// Storage id.
    pub storage_id: uuid::Uuid,
    /// Scope column.
    pub scope_column: Option<String>,
    /// Primary key columns.
    pub primary_key: Vec<String>,
}

impl RegistrationMetadata {
    /// Returns true when metadata is sufficient to activate a managed table.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        !self.table_type.is_empty()
            && !self.primary_key.is_empty()
            && (self.table_type == "shared" || self.scope_column.is_some())
    }
}
