//! Manifest creation helpers.

use chrono::Utc;

use super::model::{FilesState, Manifest};

impl Manifest {
    /// Creates a shared-table manifest.
    #[must_use]
    pub fn new_shared(
        namespace: impl Into<String>,
        table: impl Into<String>,
        schema_version: u32,
    ) -> Self {
        Self::new(namespace, table, None, schema_version)
    }

    /// Creates a user-scoped manifest.
    #[must_use]
    pub fn new_user(
        namespace: impl Into<String>,
        table: impl Into<String>,
        scope_id: impl Into<String>,
        schema_version: u32,
    ) -> Self {
        Self::new(namespace, table, Some(scope_id.into()), schema_version)
    }

    pub(super) fn new(
        namespace: impl Into<String>,
        table: impl Into<String>,
        scope_id: Option<String>,
        schema_version: u32,
    ) -> Self {
        Self {
            version: 1,
            table: table.into(),
            namespace: Some(namespace.into()),
            scope_id,
            schema_version,
            max_seq: 0,
            max_commit_seq: 0,
            updated_at: Utc::now(),
            publish: None,
            segments: Vec::new(),
            files: FilesState::default(),
        }
    }
}
