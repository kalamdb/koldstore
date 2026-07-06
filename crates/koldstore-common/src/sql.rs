//! Pg-free SQL statement planning primitives shared across workflow crates.
//!
//! Library crates build `SqlStatement` plans; `pg_koldstore` executes them through SPI.

use thiserror::Error;

/// SQL plan validation result.
pub type SqlResult<T> = Result<T, SqlError>;

/// Validation error for planned SQL statements.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("SQL {operation} failed: {message}")]
pub struct SqlError {
    /// Operation label for diagnostics.
    pub operation: String,
    /// Error message.
    pub message: String,
}

/// Maps a validation failure into a typed error.
#[must_use]
pub fn map_sql_error(operation: &str, message: &str) -> SqlError {
    SqlError {
        operation: operation.to_string(),
        message: message.to_string(),
    }
}

/// Planned statement access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlAccess {
    /// Read-only catalog access.
    ReadOnly,
    /// Read/write catalog access.
    ReadWrite,
}

/// Pg-free SQL parameter type metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlParamType {
    BigInt,
    Integer,
    Text,
    Jsonb,
    Oid,
    Uuid,
    Boolean,
}

/// Validated SQL statement metadata produced by library crates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlStatement {
    /// Human-readable operation name for diagnostics.
    pub operation: String,
    /// SQL text.
    pub sql: String,
    /// Required access mode.
    pub access: SqlAccess,
    /// Bind parameter types by one-based placeholder position.
    pub param_types: Vec<SqlParamType>,
}

impl SqlStatement {
    /// Creates a read-only statement plan.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn read(operation: &str, sql: &str) -> SqlResult<Self> {
        Self::read_with_params(operation, sql, [])
    }

    /// Creates a read-only statement plan with parameter metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn read_with_params(
        operation: &str,
        sql: &str,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> SqlResult<Self> {
        Self::new(operation, sql, SqlAccess::ReadOnly, param_types)
    }

    /// Creates a read/write statement plan.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn write(operation: &str, sql: &str) -> SqlResult<Self> {
        Self::write_with_params(operation, sql, [])
    }

    /// Creates a read/write statement plan with parameter metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn write_with_params(
        operation: &str,
        sql: &str,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> SqlResult<Self> {
        Self::new(operation, sql, SqlAccess::ReadWrite, param_types)
    }

    fn new(
        operation: &str,
        sql: &str,
        access: SqlAccess,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> SqlResult<Self> {
        let operation = operation.trim();
        let sql = sql.trim();
        if operation.is_empty() {
            return Err(map_sql_error(
                "validate statement",
                "operation cannot be empty",
            ));
        }
        if sql.is_empty() {
            return Err(map_sql_error(operation, "sql cannot be empty"));
        }

        Ok(Self {
            operation: operation.to_string(),
            sql: sql.to_string(),
            access,
            param_types: param_types.into(),
        })
    }
}
