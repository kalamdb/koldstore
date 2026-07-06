//! User-scope migration helpers.

use koldstore_common::is_safe_identifier;
use thiserror::Error;

use koldstore_common::SqlStatement;

use crate::QualifiedTableName;

const SCOPE_POLICY_NAME: &str = "koldstore_user_scope_fail_closed";

/// User-scope policy planning result.
pub type ScopeResult<T> = Result<T, ScopeError>;

/// Scope migration validation or planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScopeError {
    /// Scope column is blank or unsafe to quote as an identifier.
    #[error("invalid scope_column `{0}`")]
    InvalidScopeColumn(String),
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Planned user-scope fail-closed policy setup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserScopePolicyPlan {
    /// Scope column protected by the policy.
    pub scope_column: String,
    /// DDL statements to execute in order.
    pub statements: Vec<SqlStatement>,
}

/// Resolves the explicit application-owned scope column for a user-scoped table.
#[must_use]
pub fn effective_scope_column(table_type: &str, app_scope_column: Option<&str>) -> Option<String> {
    if table_type == "user" {
        app_scope_column
            .map(str::trim)
            .filter(|column| !column.is_empty())
            .map(ToString::to_string)
    } else {
        None
    }
}

/// Builds DDL statements that make a user-scoped table fail closed when
/// `koldstore.user_id` is missing or does not match the row scope.
///
/// # Errors
///
/// Returns an error when the scope column is not a simple safe identifier or
/// statement metadata cannot be prepared.
pub fn plan_user_scope_policy(
    table: &QualifiedTableName,
    scope_column: &str,
) -> ScopeResult<UserScopePolicyPlan> {
    let scope_column = scope_column.trim();
    if !is_safe_identifier(scope_column) {
        return Err(ScopeError::InvalidScopeColumn(scope_column.to_string()));
    }

    let table_name = table.quoted();
    let quoted_scope_column = format!("\"{scope_column}\"");
    let predicate = format!(
        "current_setting('koldstore.user_id', true) IS NOT NULL AND \
         {quoted_scope_column} = current_setting('koldstore.user_id', true)"
    );

    let statements = [
        format!("ALTER TABLE ONLY {table_name} ENABLE ROW LEVEL SECURITY"),
        format!("DROP POLICY IF EXISTS {SCOPE_POLICY_NAME} ON {table_name}"),
        format!(
            "CREATE POLICY {SCOPE_POLICY_NAME} ON {table_name} \
             AS PERMISSIVE FOR ALL USING ({predicate}) WITH CHECK ({predicate})"
        ),
    ]
    .into_iter()
    .map(|sql| SqlStatement::write("setup user scope policy", &sql))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|error| ScopeError::Spi(error.to_string()))?;

    Ok(UserScopePolicyPlan {
        scope_column: scope_column.to_string(),
        statements,
    })
}
