//! Migration request parsing and shared migration error types.

use koldstore_common::ManageTableOptions;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::rehydrate::DemigrateOptions;

/// Migration planning result.
pub type MigrationResult<T> = Result<T, MigrationError>;

/// Migration request validation or planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MigrationError {
    /// Table name is blank or not a simple qualified identifier.
    #[error("invalid table_name `{0}`")]
    InvalidTableName(String),
    /// Table type must be `shared` or `user`.
    #[error("unsupported table_type `{0}`")]
    UnsupportedTableType(String),
    /// Storage name is blank.
    #[error("storage_name cannot be blank")]
    BlankStorageName,
    /// Scope column is blank or not a simple identifier.
    #[error("invalid scope_column `{0}`")]
    InvalidScopeColumn(String),
    /// User-scoped clean-schema tables must use an application-owned scope column.
    #[error("user-scoped manage_table requires scope_column")]
    MissingScopeColumn,
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
    /// Existing-table ordering metadata is insufficient.
    #[error("{0}")]
    Ordering(String),
    /// Migration job planning failed.
    #[error("{0}")]
    Job(String),
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
    /// Optional app scope column.
    pub scope_column: Option<String>,
    /// Additional manage-table options.
    pub options: ManageTableOptions,
}

/// Demigration request from `koldstore.unmanage_table`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemigrateTableRequest {
    /// PostgreSQL relation name.
    pub table_name: String,
    /// Optional SQL argument; defaults to rehydrate.
    pub rehydrate: Option<bool>,
    /// Optional SQL argument; defaults to retaining cold artifacts.
    pub drop_cold: Option<bool>,
}

impl DemigrateTableRequest {
    /// Converts SQL optional arguments into demigration options.
    #[must_use]
    pub fn options(&self) -> DemigrateOptions {
        DemigrateOptions {
            rehydrate: self.rehydrate.unwrap_or(true),
            drop_cold: self.drop_cold.unwrap_or(false),
        }
    }
}

impl MigrateTableRequest {
    /// Returns whether automatic flush is configured through schema options.
    #[must_use]
    pub fn flush_enabled(&self) -> bool {
        self.options.flush_enabled()
    }

    /// Returns the configured hot-row limit when flush is enabled.
    #[must_use]
    pub fn hot_row_limit(&self) -> Option<u64> {
        self.options.hot_row_limit()
    }

    /// Returns the effective user scope column.
    #[must_use]
    pub fn effective_scope_column(&self) -> Option<&str> {
        if self.table_type == "user" {
            self.scope_column
                .as_deref()
                .map(str::trim)
                .filter(|scope| !scope.is_empty())
        } else {
            None
        }
    }

    /// Returns whether the request targets a supported greenfield table type.
    #[must_use]
    pub fn has_supported_table_type(&self) -> bool {
        matches!(self.table_type.as_str(), "shared" | "user")
    }

    /// Returns whether user-scope arguments are sufficient for migration.
    #[must_use]
    pub fn has_valid_scope_arguments(&self) -> bool {
        self.table_type != "user"
            || self
                .effective_scope_column()
                .map(str::trim)
                .filter(|scope| !scope.is_empty())
                .is_some()
    }
}
