//! Flush executor process helpers for crash / recovery E2E.
//!
//! Locates one-shot flush executor backends via `pg_stat_activity.backend_type`
//! (`koldstore flush executor <db_oid>`) and SIGKILLs them from the test
//! process. Prefer this over in-backend `panic:` failpoints for process-kill
//! coverage.

use anyhow::{bail, Context, Result};
use std::process::Command;
use std::time::{Duration, Instant};
use tokio_postgres::Client;

use koldstore_supervisor::flush_executor_worker_type;

/// Backend-type prefix shared with [`flush_executor_worker_type`].
pub const FLUSH_EXECUTOR_BACKEND_PREFIX: &str = "koldstore flush executor ";

/// Builds the `pg_stat_activity.backend_type` string for a database OID.
#[must_use]
pub fn flush_executor_backend_type(database_oid: u32) -> String {
    flush_executor_worker_type(koldstore_supervisor::DatabaseOid::new(database_oid))
}

/// Returns live flush executor PIDs for the current database.
///
/// # Errors
///
/// Returns an error when the activity probe fails.
pub async fn flush_executor_pids(client: &Client) -> Result<Vec<i32>> {
    let rows = client
        .query(
            "SELECT a.pid::int4 \
             FROM pg_catalog.pg_stat_activity a \
             WHERE a.datname = current_database() \
               AND a.backend_type LIKE $1",
            &[&format!("{FLUSH_EXECUTOR_BACKEND_PREFIX}%")],
        )
        .await
        .context("list flush executor pids")?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

/// Waits until at least one flush executor is visible, returning its PIDs.
///
/// # Errors
///
/// Returns an error when no executor appears before `deadline`.
pub async fn wait_for_flush_executor_pids(client: &Client, deadline: Duration) -> Result<Vec<i32>> {
    let started = Instant::now();
    loop {
        let pids = flush_executor_pids(client).await?;
        if !pids.is_empty() {
            return Ok(pids);
        }
        if started.elapsed() > deadline {
            bail!("no flush executor appeared within {deadline:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Sends SIGKILL (`kill -9`) to `pid` from the test process.
///
/// # Errors
///
/// Returns an error when the kill command fails to spawn or exits non-zero
/// for a reason other than "already dead".
pub fn sigkill_pid(pid: i32) -> Result<()> {
    let status = Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .with_context(|| format!("spawn kill -9 {pid}"))?;
    if status.success() {
        return Ok(());
    }
    // ESRCH / already reaped is acceptable after a racing exit.
    let code = status.code().unwrap_or(-1);
    if code == 1 {
        return Ok(());
    }
    bail!("kill -9 {pid} failed with status {status}");
}

/// SIGKILLs every flush executor backend for the current database.
///
/// # Errors
///
/// Returns an error when listing or killing fails.
pub async fn sigkill_flush_executors(client: &Client) -> Result<Vec<i32>> {
    let pids = flush_executor_pids(client).await?;
    for pid in &pids {
        sigkill_pid(*pid)?;
    }
    Ok(pids)
}

/// Waits until no flush executor backends remain visible.
///
/// # Errors
///
/// Returns an error when executors linger past `deadline`.
pub async fn wait_until_no_flush_executors(client: &Client, deadline: Duration) -> Result<()> {
    let started = Instant::now();
    loop {
        let pids = flush_executor_pids(client).await?;
        if pids.is_empty() {
            return Ok(());
        }
        if started.elapsed() > deadline {
            bail!("flush executors still alive after {deadline:?}: {pids:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{flush_executor_backend_type, FLUSH_EXECUTOR_BACKEND_PREFIX};

    #[test]
    fn backend_type_matches_worker_crate_identity() {
        let typed = flush_executor_backend_type(42);
        assert_eq!(typed, "koldstore flush executor 42");
        assert!(typed.starts_with(FLUSH_EXECUTOR_BACKEND_PREFIX));
    }
}
