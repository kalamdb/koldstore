//! Mirror-capture mode selected for one complete E2E suite invocation.

use anyhow::{bail, Result};

const MODE_ENV: &str = "KOLDSTORE_E2E_MIRROR_CAPTURE_MODE";

/// Mirror implementation exercised by the current E2E run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorCaptureMode {
    /// Mirror changes share the source heap transaction.
    Strict,
    /// Mirror changes are applied from committed logical WAL.
    Async,
}

impl MirrorCaptureMode {
    /// Parses the runner's stable `strict` and `async` values.
    ///
    /// # Errors
    ///
    /// Returns an error for any unsupported mode.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "strict" => Ok(Self::Strict),
            "async" => Ok(Self::Async),
            _ => bail!("invalid {MODE_ENV}={value:?}; expected strict or async"),
        }
    }

    /// Returns the SQL value accepted by `koldstore.manage_table`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Async => "async",
        }
    }

    /// Returns whether this mode needs async-only lifecycle assertions.
    #[must_use]
    pub const fn is_async(self) -> bool {
        matches!(self, Self::Async)
    }
}

/// Reads the mode selected by `scripts/run-pg-e2e.sh`.
///
/// Direct `cargo test` invocations retain the public API default of `strict`.
///
/// # Errors
///
/// Returns an error when the environment contains an unsupported value.
pub fn selected_mirror_capture_mode() -> Result<MirrorCaptureMode> {
    let value = std::env::var(MODE_ENV).unwrap_or_else(|_| "strict".to_string());
    MirrorCaptureMode::parse(&value)
}

/// Establishes a mirror-consistency boundary when the selected mode is async.
///
/// # Errors
///
/// Returns an error when mode selection or the async SQL fence fails.
pub async fn fence_selected_mirror(client: &tokio_postgres::Client) -> Result<i64> {
    if !selected_mirror_capture_mode()?.is_async() {
        return Ok(0);
    }
    Ok(client
        .query_one("SELECT koldstore.wait_for_async_mirror()", &[])
        .await?
        .get(0))
}
