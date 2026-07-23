//! Mirror-capture mode selected for one complete E2E suite invocation.

use anyhow::Result;

const MODE_ENV: &str = "KOLDSTORE_E2E_MIRROR_CAPTURE_MODE";

pub use koldstore_common::MirrorCaptureMode;

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
        .ok_or_else(|| anyhow::anyhow!("invalid {MODE_ENV}={value:?}; expected strict or async"))
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
