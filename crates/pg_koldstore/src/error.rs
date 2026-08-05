//! Adapter-layer errors for SPI and PostgreSQL boundaries.
//!
//! Library crates keep their own error types. This wrapper is the single place
//! `pg_koldstore` erases them before `pgrx::error!` / worker logging.

use std::fmt;

/// SPI / adapter failure mapped to PostgreSQL errors at boundaries.
#[derive(Debug)]
pub(crate) struct PgAdapterError(String);

impl PgAdapterError {
    /// Builds an adapter error from any displayable failure.
    pub(crate) fn from_display(error: impl fmt::Display) -> Self {
        Self(error.to_string())
    }

    /// Consumes the adapter error into its message string.
    #[must_use]
    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for PgAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PgAdapterError {}

impl From<String> for PgAdapterError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for PgAdapterError {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<crate::spi::SpiError> for PgAdapterError {
    fn from(value: crate::spi::SpiError) -> Self {
        Self(value.to_string())
    }
}

impl From<serde_json::Error> for PgAdapterError {
    fn from(value: serde_json::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<uuid::Error> for PgAdapterError {
    fn from(value: uuid::Error) -> Self {
        Self(value.to_string())
    }
}

/// Result alias for adapter-layer fallible work.
pub(crate) type PgResult<T> = Result<T, PgAdapterError>;

impl From<PgAdapterError> for String {
    fn from(value: PgAdapterError) -> Self {
        value.into_string()
    }
}
