//! Safe SPI helper boundary.

use std::sync::Mutex;

use thiserror::Error;

/// SQLSTATE used for pg-koldstore errors.
pub const KOLDSTORE_SQLSTATE: &str = "XXKLD";

/// SPI helper result.
pub type SpiResult<T> = Result<T, SpiError>;

/// Mapped SPI error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("SPI {operation} failed: {message}")]
pub struct SpiError {
    /// SPI operation name.
    pub operation: String,
    /// Error message.
    pub message: String,
}

/// Maps a SPI failure into a typed error.
#[must_use]
pub fn map_spi_error(operation: &str, message: &str) -> SpiError {
    SpiError {
        operation: operation.to_string(),
        message: message.to_string(),
    }
}

/// SPI access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpiAccess {
    /// Read-only catalog access.
    ReadOnly,
    /// Read/write catalog access.
    ReadWrite,
}

/// Validated SPI statement metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiStatement {
    /// Human-readable operation name for diagnostics.
    pub operation: String,
    /// SQL text.
    pub sql: String,
    /// Required SPI access mode.
    pub access: SpiAccess,
}

impl SpiStatement {
    /// Creates a read-only catalog statement.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn read(operation: &str, sql: &str) -> SpiResult<Self> {
        Self::new(operation, sql, SpiAccess::ReadOnly)
    }

    /// Creates a read/write catalog statement.
    ///
    /// # Errors
    ///
    /// Returns an error when operation or SQL text is blank.
    pub fn write(operation: &str, sql: &str) -> SpiResult<Self> {
        Self::new(operation, sql, SpiAccess::ReadWrite)
    }

    fn new(operation: &str, sql: &str, access: SpiAccess) -> SpiResult<Self> {
        let operation = operation.trim();
        let sql = sql.trim();
        if operation.is_empty() {
            return Err(map_spi_error(
                "validate statement",
                "operation cannot be empty",
            ));
        }
        if sql.is_empty() {
            return Err(map_spi_error(operation, "sql cannot be empty"));
        }

        Ok(Self {
            operation: operation.to_string(),
            sql: sql.to_string(),
            access,
        })
    }
}

/// SPI execution result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpiRows {
    /// Number of affected rows when known.
    pub rows_affected: u64,
}

/// Testable SPI executor boundary.
pub trait SpiExecutor {
    /// Executes a validated statement.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying SPI call fails.
    fn execute(&self, statement: SpiStatement) -> SpiResult<SpiRows>;
}

/// Executes a read-only catalog statement.
///
/// # Errors
///
/// Returns an error if the statement is not read-only or execution fails.
pub fn execute_catalog_read(
    executor: &impl SpiExecutor,
    statement: SpiStatement,
) -> SpiResult<SpiRows> {
    if statement.access != SpiAccess::ReadOnly {
        return Err(map_spi_error(
            &statement.operation,
            "read helper requires read-only statement",
        ));
    }
    executor.execute(statement)
}

/// Executes a read/write catalog statement.
///
/// # Errors
///
/// Returns an error if the statement is not read/write or execution fails.
pub fn execute_catalog_write(
    executor: &impl SpiExecutor,
    statement: SpiStatement,
) -> SpiResult<SpiRows> {
    if statement.access != SpiAccess::ReadWrite {
        return Err(map_spi_error(
            &statement.operation,
            "write helper requires read/write statement",
        ));
    }
    executor.execute(statement)
}

/// Recording executor used by pure Rust tests.
#[derive(Debug, Default)]
pub struct RecordingSpiExecutor {
    statements: Mutex<Vec<SpiStatement>>,
}

impl RecordingSpiExecutor {
    /// Returns the recorded statement sequence.
    #[must_use]
    pub fn statements(&self) -> Vec<SpiStatement> {
        self.statements
            .lock()
            .map(|statements| statements.clone())
            .unwrap_or_default()
    }
}

impl SpiExecutor for RecordingSpiExecutor {
    fn execute(&self, statement: SpiStatement) -> SpiResult<SpiRows> {
        self.statements
            .lock()
            .map_err(|_| map_spi_error(&statement.operation, "recording executor lock poisoned"))?
            .push(statement);
        Ok(SpiRows { rows_affected: 1 })
    }
}

/// PostgreSQL SPI executor.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
#[derive(Debug, Default)]
pub struct PgSpiExecutor;

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
impl SpiExecutor for PgSpiExecutor {
    fn execute(&self, statement: SpiStatement) -> SpiResult<SpiRows> {
        let rows = match statement.access {
            SpiAccess::ReadOnly => pgrx::Spi::connect(|client| {
                client
                    .select(&statement.sql, None, &[])
                    .map(|tuples| SpiRows {
                        rows_affected: tuples.len() as u64,
                    })
            }),
            SpiAccess::ReadWrite => pgrx::Spi::connect_mut(|client| {
                client
                    .update(&statement.sql, None, &[])
                    .map(|tuples| SpiRows {
                        rows_affected: tuples.len() as u64,
                    })
            }),
        }
        .map_err(|error| map_spi_error(&statement.operation, &error.to_string()))?;
        Ok(rows)
    }
}
