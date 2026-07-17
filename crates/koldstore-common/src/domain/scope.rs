//! User-scope enforcement helpers shared across workflow crates.

use crate::{is_safe_identifier, quote_ident, ScopeKey, TableKind};
use thiserror::Error;

/// Scope enforcement error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScopeError {
    /// User-scoped table access has no active session scope.
    #[error("koldstore.user_id is not set")]
    MissingUserId,
    /// A user-scoped row or metadata entry is missing its scope key.
    #[error("row scope is missing")]
    MissingRowScope,
    /// The row belongs to a different scope than the active session.
    #[error("row scope `{row_scope}` does not match koldstore.user_id `{active_scope}`")]
    CrossScope {
        /// Active `koldstore.user_id`.
        active_scope: String,
        /// Scope stored on the row or metadata entry.
        row_scope: String,
    },
}

/// Scope SQL helper error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid scope SQL identifier `{0}`")]
pub struct ScopeSqlError(String);

/// Normalizes a user scope string.
#[must_use]
pub fn normalize_scope(value: &str) -> String {
    value.trim().to_string()
}

/// Requires a user scope and returns the normalized value.
///
/// # Errors
///
/// Returns an error when the scope is missing or empty.
pub fn require_user_scope(value: Option<&str>) -> Result<String, String> {
    let Some(value) = value.map(normalize_scope).filter(|value| !value.is_empty()) else {
        return Err("koldstore.user_id is not set".to_string());
    };
    Ok(value)
}

/// Returns whether a row scope matches the active session scope.
#[must_use]
pub fn scope_matches(active_scope: &str, row_scope: &str) -> bool {
    normalize_scope(active_scope) == normalize_scope(row_scope)
}

/// Resolves the active scope required for a managed table kind.
///
/// # Errors
///
/// Returns [`ScopeError::MissingUserId`] for user-scoped tables when the session
/// has no non-empty `koldstore.user_id`.
pub fn active_scope_for_table(
    table_kind: TableKind,
    session_user_id: Option<&str>,
) -> Result<Option<ScopeKey>, ScopeError> {
    match table_kind {
        TableKind::Shared => Ok(None),
        TableKind::User => {
            let scope =
                require_user_scope(session_user_id).map_err(|_| ScopeError::MissingUserId)?;
            ScopeKey::new(scope)
                .map(Some)
                .map_err(|_| ScopeError::MissingUserId)
        }
    }
}

/// Verifies a user-scoped row belongs to the active session scope.
///
/// # Errors
///
/// Returns [`ScopeError::MissingRowScope`] when the row has no scope and
/// [`ScopeError::CrossScope`] when it belongs to another user scope.
pub fn enforce_row_scope(
    active_scope: &ScopeKey,
    row_scope: Option<&ScopeKey>,
) -> Result<(), ScopeError> {
    let Some(row_scope) = row_scope else {
        return Err(ScopeError::MissingRowScope);
    };
    if scope_matches(active_scope.as_str(), row_scope.as_str()) {
        Ok(())
    } else {
        Err(ScopeError::CrossScope {
            active_scope: active_scope.to_string(),
            row_scope: row_scope.to_string(),
        })
    }
}

/// Builds a SQL predicate that compares an aliased application-owned scope
/// column to a bind parameter.
///
/// # Errors
///
/// Returns an error when the scope column or alias is not a simple identifier.
pub fn scope_predicate_sql(
    relation_alias: &str,
    scope_column: &str,
    parameter_index: usize,
) -> Result<String, ScopeSqlError> {
    let alias = validate_identifier(relation_alias)?;
    let column = validate_identifier(scope_column)?;
    Ok(format!(
        "{alias}.{column}::text = ${parameter_index}::text",
        alias = quote_ident(alias),
        column = quote_ident(column),
    ))
}

fn validate_identifier(value: &str) -> Result<&str, ScopeSqlError> {
    let value = value.trim();
    if is_safe_identifier(value) {
        Ok(value)
    } else {
        Err(ScopeSqlError(value.to_string()))
    }
}
