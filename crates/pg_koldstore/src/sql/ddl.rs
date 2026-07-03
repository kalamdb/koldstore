//! Public DDL SQL function boundaries.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Registers storage and returns its id.
#[must_use]
pub fn register_storage_name_only(_name: &str) -> Uuid {
    Uuid::new_v4()
}

/// Storage registration request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageRegistration {
    /// Unique storage name.
    pub name: String,
    /// Backend type.
    pub storage_type: String,
    /// Base object-store path.
    pub base_path: String,
    /// Redacted or raw credentials depending on caller privileges.
    pub credentials: serde_json::Value,
    /// Backend config.
    pub config: serde_json::Value,
    /// Shared table path template.
    pub shared_path_template: String,
    /// User table path template.
    pub user_path_template: String,
}

impl StorageRegistration {
    /// Returns a credential-redacted copy for application-role diagnostics.
    #[must_use]
    pub fn redacted(&self) -> Self {
        let mut copy = self.clone();
        copy.credentials = serde_json::json!({"redacted": true});
        copy
    }
}

/// Migration request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrateTableRequest {
    /// PostgreSQL relation name.
    pub table_name: String,
    /// `shared` or `user`.
    pub table_type: String,
    /// Storage registration name.
    pub storage_name: String,
    /// Optional flush policy.
    pub flush_policy: Option<String>,
    /// Optional app scope column.
    pub scope_column: Option<String>,
    /// Additional options.
    pub options: serde_json::Value,
}

impl MigrateTableRequest {
    /// Returns the effective user scope column.
    #[must_use]
    pub fn effective_scope_column(&self) -> Option<&str> {
        if self.table_type == "user" {
            Some(self.scope_column.as_deref().unwrap_or("_user_id"))
        } else {
            None
        }
    }
}
