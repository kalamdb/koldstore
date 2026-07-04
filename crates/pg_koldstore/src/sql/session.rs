//! Session SQL helpers.

use thiserror::Error;

use super::snowflake;

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
#[cfg_attr(feature = "pg", pgrx::pg_extern(name = "snowflake_id"))]
pub fn snowflake_id() -> i64 {
    snowflake::next_id(snowflake_worker_id()).unwrap_or_else(raise_snowflake_error)
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
#[cfg_attr(feature = "pg", pgrx::pg_extern(name = "koldstore_user_id"))]
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

#[cfg(feature = "pg_test")]
const fn snowflake_worker_id() -> u16 {
    0
}

#[cfg(all(not(feature = "pg_test"), any(feature = "pg17", feature = "pg18")))]
fn snowflake_worker_id() -> u16 {
    let proc_number = unsafe { pgrx::pg_sys::MyProcNumber };
    normalize_postgres_worker_id(proc_number)
}

#[cfg(all(not(feature = "pg_test"), any(feature = "pg15", feature = "pg16")))]
fn snowflake_worker_id() -> u16 {
    let backend_id = unsafe { pgrx::pg_sys::MyBackendId };
    normalize_postgres_worker_id(backend_id)
}

#[cfg(not(any(feature = "pg", feature = "pg_test")))]
const fn snowflake_worker_id() -> u16 {
    0
}

#[cfg(feature = "pg")]
fn normalize_postgres_worker_id(worker_id: i32) -> u16 {
    if worker_id <= 0 {
        return 0;
    }
    let worker_id = u16::try_from(worker_id)
        .unwrap_or_else(|_| pgrx::error!("PostgreSQL backend worker id {worker_id} is too large"));
    if worker_id > 1023 {
        pgrx::error!("PostgreSQL backend worker id {worker_id} exceeds Snowflake worker id limit");
    }
    worker_id
}

#[cfg(feature = "pg")]
fn raise_snowflake_error(error: snowflake::SnowflakeError) -> i64 {
    pgrx::error!("snowflake id generation failed: {error}")
}

#[cfg(not(feature = "pg"))]
fn raise_snowflake_error(error: snowflake::SnowflakeError) -> i64 {
    panic!("snowflake id generation failed: {error}")
}
