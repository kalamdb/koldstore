//! Shared helpers for async mirror worker E2E assertions.

use anyhow::Result;
use std::time::{Duration, Instant};

const WORKER_START_DEADLINE: Duration = Duration::from_secs(30);
const BACKGROUND_APPLY_DEADLINE: Duration = Duration::from_secs(10);

/// Waits until the async mirror database worker is visible in `pg_stat_activity`.
///
/// # Errors
///
/// Returns an error when ensure fails or the worker is not visible in time.
pub async fn wait_for_async_worker(client: &tokio_postgres::Client) -> Result<Duration> {
    let started = Instant::now();
    loop {
        client
            .query_one(
                "SELECT koldstore.internal_ensure_async_mirror_worker()",
                &[],
            )
            .await?;
        if async_worker_running(client).await? {
            return Ok(started.elapsed());
        }
        anyhow::ensure!(
            started.elapsed() <= WORKER_START_DEADLINE,
            "async WAL applier did not become visible within {WORKER_START_DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Returns whether the current database's async mirror worker is running.
///
/// # Errors
///
/// Returns an error when the activity probe fails.
pub async fn async_worker_running(client: &tokio_postgres::Client) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT EXISTS (\
               SELECT 1 FROM pg_catalog.pg_stat_activity \
               WHERE backend_type = 'koldstore async mirror ' \
                 || (SELECT oid::text FROM pg_catalog.pg_database \
                     WHERE datname = current_database())\
             )",
            &[],
        )
        .await?
        .get(0))
}

/// Terminates the async mirror worker for the current database, if any.
///
/// Waits until the worker is no longer visible in `pg_stat_activity`. Callers
/// that then touch the logical slot (flush fence / peek) still rely on the
/// extension waiting out PostgreSQL's post-abort slot-release window.
///
/// # Errors
///
/// Returns an error when termination SQL fails or the worker does not exit in
/// time.
pub async fn terminate_async_worker(client: &tokio_postgres::Client) -> Result<bool> {
    let terminated = client
        .query_one(
            "SELECT COALESCE((\
               SELECT pg_terminate_backend(pid) \
               FROM pg_catalog.pg_stat_activity \
               WHERE backend_type = 'koldstore async mirror ' \
                 || (SELECT oid::text FROM pg_catalog.pg_database \
                     WHERE datname = current_database()) \
               LIMIT 1\
             ), false)",
            &[],
        )
        .await?
        .get(0);
    let started = Instant::now();
    while async_worker_running(client).await? {
        anyhow::ensure!(
            started.elapsed() <= WORKER_START_DEADLINE,
            "async WAL applier did not exit within {WORKER_START_DEADLINE:?} after terminate"
        );
        // Re-signal in case SIGTERM landed during a non-interruptible window.
        let _ = client
            .query_one(
                "SELECT COALESCE((\
                   SELECT pg_terminate_backend(pid) \
                   FROM pg_catalog.pg_stat_activity \
                   WHERE backend_type = 'koldstore async mirror ' \
                     || (SELECT oid::text FROM pg_catalog.pg_database \
                         WHERE datname = current_database()) \
                   LIMIT 1\
                 ), false)",
                &[],
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(terminated)
}

/// Counts mirror rows with the given operation code.
///
/// # Errors
///
/// Returns an error when the count query fails.
pub async fn mirror_op_count(
    client: &tokio_postgres::Client,
    mirror: &str,
    op: i16,
) -> Result<i64> {
    Ok(client
        .query_one(
            &format!("SELECT count(*) FROM {mirror} WHERE op = $1"),
            &[&op],
        )
        .await?
        .get(0))
}

/// Waits until the mirror has `expected` rows with operation `op`.
///
/// Drives catch-up via [`wait_for_async_mirror`] so progress does not depend
/// solely on the background worker remaining alive between polls (important
/// after failpoint/kill churn in the same suite).
///
/// # Errors
///
/// Returns an error when the deadline elapses, apply fails, or queries fail.
pub async fn wait_for_mirror_op_count(
    client: &tokio_postgres::Client,
    mirror: &str,
    op: i16,
    expected: i64,
) -> Result<()> {
    let started = Instant::now();
    loop {
        if mirror_op_count(client, mirror, op).await? == expected {
            return Ok(());
        }
        anyhow::ensure!(
            started.elapsed() <= BACKGROUND_APPLY_DEADLINE,
            "timed out after {BACKGROUND_APPLY_DEADLINE:?} waiting for {expected} mirror rows with op={op}"
        );
        // Frontend fence applies available WAL even when the background worker
        // is mid-restart after a prior test's kill/failpoint.
        wait_for_async_mirror(client).await?;
        if mirror_op_count(client, mirror, op).await? == expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Explicit async fence.
///
/// # Errors
///
/// Returns an error when `wait_for_async_mirror` fails.
pub async fn wait_for_async_mirror(client: &tokio_postgres::Client) -> Result<i64> {
    Ok(client
        .query_one("SELECT koldstore.wait_for_async_mirror()", &[])
        .await?
        .get(0))
}

/// Slot + durable apply watermark for idle-path assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsyncMirrorProgress {
    pub confirmed_flush_lsn: String,
    pub retained_bytes: i64,
    pub applied_lsn: Option<String>,
    pub updated_at: Option<String>,
}

/// Reads replication-slot retention and `async_mirror_state` for this database.
///
/// # Errors
///
/// Returns an error when the slot is missing or the probe queries fail.
pub async fn async_mirror_progress(client: &tokio_postgres::Client) -> Result<AsyncMirrorProgress> {
    let row = client
        .query_one(
            "SELECT \
               s.confirmed_flush_lsn::text, \
               pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)::bigint, \
               st.applied_lsn::text, \
               st.updated_at::text \
             FROM pg_catalog.pg_replication_slots s \
             LEFT JOIN koldstore.async_mirror_state st \
               ON st.database_oid = (SELECT oid FROM pg_catalog.pg_database \
                                     WHERE datname = current_database()) \
             WHERE s.slot_name = koldstore.async_mirror_slot_name()",
            &[],
        )
        .await?;
    Ok(AsyncMirrorProgress {
        confirmed_flush_lsn: row.get(0),
        retained_bytes: row.get(1),
        applied_lsn: row.get(2),
        updated_at: row.get(3),
    })
}

/// Byte distance between two LSNs (`pg_wal_lsn_diff(newer, older)`).
///
/// # Errors
///
/// Returns an error when the LSN cast or diff probe fails.
pub async fn wal_lsn_diff_bytes(
    client: &tokio_postgres::Client,
    newer_lsn: &str,
    older_lsn: &str,
) -> Result<i64> {
    Ok(client
        .query_one(
            "SELECT pg_wal_lsn_diff($1::text::pg_lsn, $2::text::pg_lsn)::bigint",
            &[&newer_lsn, &older_lsn],
        )
        .await?
        .get(0))
}

/// Current insert LSN as text (`pg_current_wal_lsn()`).
///
/// # Errors
///
/// Returns an error when the probe fails.
pub async fn current_wal_lsn(client: &tokio_postgres::Client) -> Result<String> {
    Ok(client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0))
}

/// Waits until `confirmed_flush_lsn` moves past `before_lsn`.
///
/// Used to assert empty peeks advance the slot past non-publication WAL.
///
/// # Errors
///
/// Returns an error when the deadline elapses or probes fail.
pub async fn wait_for_confirmed_flush_past(
    client: &tokio_postgres::Client,
    before_lsn: &str,
    deadline: Duration,
) -> Result<AsyncMirrorProgress> {
    wait_for_confirmed_flush_cmp(client, before_lsn, ">", deadline).await
}

/// Waits until `confirmed_flush_lsn` reaches at least `target_lsn`.
///
/// Prefer this after accumulating non-publication WAL: wait until the slot has
/// caught the post-noise horizon, not merely any advance past an older baseline.
/// Absolute `retained_bytes` is not a reliable gate under parallel e2e WAL.
///
/// # Errors
///
/// Returns an error when the deadline elapses or probes fail.
pub async fn wait_for_confirmed_flush_at_least(
    client: &tokio_postgres::Client,
    target_lsn: &str,
    deadline: Duration,
) -> Result<AsyncMirrorProgress> {
    wait_for_confirmed_flush_cmp(client, target_lsn, ">=", deadline).await
}

async fn wait_for_confirmed_flush_cmp(
    client: &tokio_postgres::Client,
    bound_lsn: &str,
    op: &str,
    deadline: Duration,
) -> Result<AsyncMirrorProgress> {
    let started = Instant::now();
    let sql = format!("SELECT $1::text::pg_lsn {op} $2::text::pg_lsn");
    loop {
        let progress = async_mirror_progress(client).await?;
        let ready: bool = client
            .query_one(&sql, &[&progress.confirmed_flush_lsn, &bound_lsn])
            .await?
            .get(0);
        if ready {
            return Ok(progress);
        }
        anyhow::ensure!(
            started.elapsed() <= deadline,
            "confirmed_flush_lsn did not satisfy {op} {bound_lsn} within {deadline:?} \
             (still {}, retained_bytes={})",
            progress.confirmed_flush_lsn,
            progress.retained_bytes
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Fence until mirror catch-up for WAL-only capture.
///
/// Call this before assertions that inspect `__cl` contents or merge-scan
/// overlays that depend on the latest-state mirror.
///
/// # Errors
///
/// Returns an error when the async fence fails.
pub async fn fence_async_mirror(client: &tokio_postgres::Client) -> Result<i64> {
    wait_for_async_mirror(client).await
}
