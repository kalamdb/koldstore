//! Safe SPI helper boundary.

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
