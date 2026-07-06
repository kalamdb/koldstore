//! Session SQL helper constants and pure planning helpers.

use crate::is_safe_identifier;

/// Public SQL function name used for Snowflake-style ids.
pub const SNOWFLAKE_ID_FUNCTION: &str = "SNOWFLAKE_ID";

/// Session SQL helper result.
pub type SessionSqlResult<T> = Result<T, SessionSqlError>;

/// Session SQL helper validation error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionSqlError {
    /// Identifier is blank or unsafe to quote.
    #[error("invalid identifier `{0}`")]
    InvalidIdentifier(String),
}

/// Returns the SQL default expression for application ids and mirror sequence values.
#[must_use]
pub const fn snowflake_default_expression() -> &'static str {
    "SNOWFLAKE_ID()"
}

/// Returns the schema-qualified Snowflake call for restricted `search_path` contexts.
#[must_use]
pub const fn snowflake_id_call_expression() -> &'static str {
    "public.snowflake_id()"
}

/// Builds a greenfield bigint primary-key column clause using `SNOWFLAKE_ID()`.
///
/// # Errors
///
/// Returns an error when `column_name` is not a simple safe identifier.
pub fn primary_key_default_clause(column_name: &str) -> SessionSqlResult<String> {
    let column_name = column_name.trim();
    if !is_safe_identifier(column_name) {
        return Err(SessionSqlError::InvalidIdentifier(column_name.to_string()));
    }

    Ok(format!(
        "\"{column_name}\" bigint PRIMARY KEY DEFAULT {}",
        snowflake_default_expression()
    ))
}

/// Normalizes an optional session user id.
#[must_use]
pub fn normalize_user_id(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
