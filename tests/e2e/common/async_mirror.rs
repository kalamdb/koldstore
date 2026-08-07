//! Shared helpers for async mirror worker E2E assertions.

use anyhow::Result;
use std::time::{Duration, Instant};

const WORKER_START_DEADLINE: Duration = Duration::from_secs(30);
const BACKGROUND_APPLY_DEADLINE: Duration = Duration::from_secs(30);
const WORKER_OBSERVE_DEADLINE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AsyncMaintenanceState {
    registered: bool,
    running: bool,
    starting: bool,
    pending: bool,
    recovery_requested: bool,
    wal_generation: i64,
    wal_processed_generation: i64,
    maintenance_generation: i64,
    maintenance_processed_generation: i64,
}

impl AsyncMaintenanceState {
    fn caught_up(self) -> bool {
        self.registered
            && !self.pending
            && self.wal_generation == self.wal_processed_generation
            && self.maintenance_generation == self.maintenance_processed_generation
    }
}

async fn async_maintenance_state(
    client: &tokio_postgres::Client,
) -> Result<AsyncMaintenanceState> {
    let row = client
        .query_one(
            r#"
            SELECT
              COALESCE((status->'maintenance'->>'registered')::boolean, false),
              COALESCE((status->'maintenance'->>'running')::boolean, false),
              COALESCE((status->'maintenance'->>'starting')::boolean, false),
              COALESCE((status->'maintenance'->>'pending')::boolean, false),
              COALESCE((status->'maintenance'->>'recovery_requested')::boolean, false),
              COALESCE((status->'maintenance'->>'wal_generation')::bigint, 0),
              COALESCE((status->'maintenance'->>'wal_processed_generation')::bigint, 0),
              COALESCE((status->'maintenance'->>'maintenance_generation')::bigint, 0),
              COALESCE((status->'maintenance'->>'maintenance_processed_generation')::bigint, 0)
            FROM (SELECT koldstore.async_mirror_status() AS status) s
            "#,
            &[],
        )
        .await?;
    Ok(AsyncMaintenanceState {
        registered: row.get(0),
        running: row.get(1),
        starting: row.get(2),
        pending: row.get(3),
        recovery_requested: row.get(4),
        wal_generation: row.get(5),
        wal_processed_generation: row.get(6),
        maintenance_generation: row.get(7),
        maintenance_processed_generation: row.get(8),
    })
}

/// Requests database maintenance and waits until the event-driven subsystem has
/// either started the ephemeral worker or fully consumed the published request.
///
/// A healthy KoldStore database normally has no maintenance process in
/// `pg_stat_activity`: workers are one-shot/burst processes and exit after a
/// short idle grace. Tests therefore assert shared generations rather than
/// requiring a permanently visible process.
///
/// # Errors
///
/// Returns an error when the request fails or the supervisor does not acknowledge
/// the database within [`WORKER_START_DEADLINE`].
pub async fn wait_for_async_worker(client: &tokio_postgres::Client) -> Result<Duration> {
    release_async_worker_stop_lock(client).await?;
    let started = Instant::now();
    loop {
        client
            .query_one(
                "SELECT koldstore.internal_ensure_async_mirror_worker()",
                &[],
            )
            .await?;
        let state = async_maintenance_state(client).await?;
        if state.registered && (state.running || state.starting || state.caught_up()) {
            return Ok(started.elapsed());
        }
        anyhow::ensure!(
            started.elapsed() <= WORKER_START_DEADLINE,
            "async maintenance was not acknowledged within {WORKER_START_DEADLINE:?}; state={state:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Waits for supervisor-owned recovery after a maintenance process was killed.
///
/// This deliberately does not call `internal_ensure_async_mirror_worker`: the
/// child lifecycle signal / safety reconciliation must make the shared state
/// healthy again on its own. Because the replacement process can finish before
/// a test samples `pg_stat_activity`, a caught-up generation is also success.
///
/// # Errors
///
/// Returns an error when supervisor recovery does not settle before the deadline.
pub async fn wait_for_async_worker_auto_restart(
    client: &tokio_postgres::Client,
) -> Result<Duration> {
    let started = Instant::now();
    loop {
        let state = async_maintenance_state(client).await?;
        if state.registered && (state.running || state.starting || state.caught_up()) {
            return Ok(started.elapsed());
        }
        anyhow::ensure!(
            started.elapsed() <= WORKER_START_DEADLINE,
            "async maintenance did not recover within {WORKER_START_DEADLINE:?}; state={state:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Returns whether the current database has a maintenance process visible in
/// `pg_stat_activity` at this instant.
///
/// This is intentionally only a transient process probe. Use generation-aware
/// wait helpers when testing subsystem health.
///
/// # Errors
///
/// Returns an error when the activity probe fails.
pub async fn async_worker_running(client: &tokio_postgres::Client) -> Result<bool> {
    maintenance_process_running(client).await
}

async fn maintenance_process_running(client: &tokio_postgres::Client) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT EXISTS (\
               SELECT 1 FROM pg_catalog.pg_stat_activity \
               WHERE backend_type = 'koldstore maintenance ' \
                 || (SELECT oid::text FROM pg_catalog.pg_database \
                     WHERE datname = current_database())\
             )",
            &[],
        )
        .await?
        .get(0))
}

async fn signal_maintenance_process(client: &tokio_postgres::Client) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT COALESCE((\
               SELECT pg_terminate_backend(pid) \
               FROM pg_catalog.pg_stat_activity \
               WHERE backend_type = 'koldstore maintenance ' \
                 || (SELECT oid::text FROM pg_catalog.pg_database \
                     WHERE datname = current_database()) \
               LIMIT 1\
             ), false)",
            &[],
        )
        .await?
        .get(0))
}

/// Terminates an ephemeral maintenance worker for the current database.
///
/// If the database is healthy and idle, there may be no process to kill. In that
/// case this helper publishes one diagnostic maintenance request and briefly
/// waits for its process so crash-recovery tests still inject a real SIGTERM.
/// When dispatch is paused, the diagnostic request is rejected and the function
/// simply reports that no worker existed.
///
/// # Errors
///
/// Returns an error when termination SQL fails or a signalled worker does not exit.
pub async fn terminate_async_worker(client: &tokio_postgres::Client) -> Result<bool> {
    let mut terminated = signal_maintenance_process(client).await?;
    if !terminated {
        let requested: bool = client
            .query_one(
                "SELECT koldstore.internal_ensure_async_mirror_worker()",
                &[],
            )
            .await?
            .get(0);
        if requested {
            let observe_started = Instant::now();
            while observe_started.elapsed() <= WORKER_OBSERVE_DEADLINE {
                if maintenance_process_running(client).await? {
                    terminated = signal_maintenance_process(client).await?;
                    if terminated {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }

    if !terminated {
        return Ok(false);
    }

    let started = Instant::now();
    while maintenance_process_running(client).await? {
        anyhow::ensure!(
            started.elapsed() <= WORKER_START_DEADLINE,
            "async maintenance process did not exit within {WORKER_START_DEADLINE:?} after terminate"
        );
        // Re-signal in case SIGTERM landed during a non-interruptible window.
        let _ = signal_maintenance_process(client).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(true)
}

/// Releases a pause taken by [`force_stop_async_worker`].
///
/// # Errors
///
/// Returns an error when the resume SQL fails.
pub async fn release_async_worker_stop_lock(client: &tokio_postgres::Client) -> Result<()> {
    client
        .query_one(
            "SELECT koldstore.internal_set_async_mirror_ensure_paused(false)",
            &[],
        )
        .await?;
    Ok(())
}

/// Pauses supervisor maintenance dispatch and terminates any live process.
///
/// PostgreSQL advisory locks are database-local, so the test control uses the
/// extension's shared-memory pause set. Call [`wait_for_async_worker`] or
/// [`release_async_worker_stop_lock`] when maintenance should resume.
///
/// # Errors
///
/// Returns an error when pause/terminate fails or a process keeps coming back.
pub async fn force_stop_async_worker(client: &tokio_postgres::Client) -> Result<()> {
    client
        .query_one(
            "SELECT koldstore.internal_set_async_mirror_ensure_paused(true)",
            &[],
        )
        .await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let _ = terminate_async_worker(client).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        if !maintenance_process_running(client).await? {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "async maintenance process did not stay stopped within 10s"
        );
    }
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

/// Passively waits until autonomous background maintenance has produced the
/// expected mirror row count.
///
/// This helper deliberately does **not** call `wait_for_async_mirror()`. A
/// frontend fence applies WAL itself and would make worker/supervisor reliability
/// tests pass even if automatic dispatch were broken.
///
/// # Errors
///
/// Returns an error when the deadline elapses or count probes fail.
pub async fn wait_for_mirror_op_count(
    client: &tokio_postgres::Client,
    mirror: &str,
    op: i16,
    expected: i64,
) -> Result<()> {
    let started = Instant::now();
    loop {
        let actual = mirror_op_count(client, mirror, op).await?;
        if actual == expected {
            return Ok(());
        }
        let state = async_maintenance_state(client).await?;
        anyhow::ensure!(
            started.elapsed() <= BACKGROUND_APPLY_DEADLINE,
            "timed out after {BACKGROUND_APPLY_DEADLINE:?} waiting for {expected} mirror rows with op={op}; actual={actual}, maintenance={state:?}"
        );
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
             WHERE s.slot_name = (koldstore.async_mirror_status()->>'slot_name')",
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
