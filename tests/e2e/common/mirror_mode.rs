//! Async mirror fence helpers for E2E suites.
//!
//! Capture is always committed-WAL; tests fence with `wait_for_async_mirror`
//! whenever they need an exact mirror boundary.

use anyhow::Result;

/// Establishes a mirror-consistency boundary after source commits.
///
/// # Errors
///
/// Returns an error when the async SQL fence fails.
pub async fn fence_selected_mirror(client: &tokio_postgres::Client) -> Result<i64> {
    Ok(client
        .query_one("SELECT koldstore.wait_for_async_mirror()", &[])
        .await?
        .get(0))
}

/// Compatibility alias used by older call sites.
pub async fn fence_async_mirror(client: &tokio_postgres::Client) -> Result<i64> {
    fence_selected_mirror(client).await
}
