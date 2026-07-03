//! Session SQL helpers.

use std::sync::atomic::{AtomicI64, Ordering};
use thiserror::Error;

static NEXT_SEQ: AtomicI64 = AtomicI64::new(1);

/// Public SQL function name used for Snowflake-style ids.
pub const SNOWFLAKE_ID_FUNCTION: &str = "SNOWFLAKE_ID";

/// Session SQL helper result.
pub type SessionSqlResult<T> = Result<T, SessionSqlError>;

/// Session SQL helper validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SessionSqlError {
    /// Identifier is blank or unsafe to quote.
    #[error("invalid identifier `{0}`")]
    InvalidIdentifier(String),
}

/// Generates a monotonic Snowflake-like id for tests and SQL default use.
#[must_use]
#[cfg_attr(
    any(feature = "pg15", feature = "pg16", feature = "pg17"),
    pgrx::pg_extern(name = "SNOWFLAKE_ID")
)]
pub fn snowflake_id() -> i64 {
    NEXT_SEQ.fetch_add(1, Ordering::SeqCst)
}

/// Returns the SQL default expression for application ids and `_seq`.
#[must_use]
pub const fn snowflake_default_expression() -> &'static str {
    "SNOWFLAKE_ID()"
}

/// Returns the SQL default clause used by pg-koldstore `_seq` columns.
#[must_use]
pub const fn system_seq_default_clause() -> &'static str {
    "DEFAULT SNOWFLAKE_ID()"
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
        "\"{column_name}\" bigint PRIMARY KEY {}",
        system_seq_default_clause()
    ))
}

/// Returns the active user scope when available.
#[must_use]
#[cfg_attr(
    any(feature = "pg15", feature = "pg16", feature = "pg17"),
    pgrx::pg_extern(name = "koldstore_user_id")
)]
pub fn koldstore_user_id() -> Option<String> {
    None
}

/// Normalizes an optional session user id.
#[must_use]
pub fn normalize_user_id(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
