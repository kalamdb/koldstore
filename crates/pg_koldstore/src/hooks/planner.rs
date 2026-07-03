//! Planner hook integration for KoldstoreMergeScan.

use koldstore_core::{ScopeKey, TableKind};

use crate::security::scope::{self, ScopeError};

/// Name shown in `EXPLAIN`.
pub const MERGE_SCAN_NAME: &str = "KoldstoreMergeScan";

/// Resolves the plan-time scope key for a managed read.
///
/// # Errors
///
/// Returns [`ScopeError::MissingUserId`] for user-scoped tables before any hot
/// heap or cold object path is planned.
pub fn plan_scope_key_for_read(
    table_kind: TableKind,
    session_user_id: Option<&str>,
) -> Result<Option<ScopeKey>, ScopeError> {
    scope::active_scope_for_table(table_kind, session_user_id)
}
