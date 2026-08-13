//! Shared helpers for persistent WAL-applier E2E assertions.

use anyhow::Result;
use std::time::{Duration, Instant};

const WORKER_START_DEADLINE: Duration = Duration::from_secs(30);
const BACKGROUND_APPLY_DEADLINE: Duration = Duration::from_secs(30);
const WORKER_OBSERVE_DEADLINE: Duration = Duration::from_secs(2);

/// Fully observational WAL/flush state used by autonomous liveness tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassiveKoldStoreState {
    pub wal_pid: Option<i32>,
    pub wal_required: bool,
    pub wal_pending: bool,
    pub wal_generation: i64,
    pub wal_processed_generation: i64,
    pub completed_jobs: i64,
    pub active_jobs: i64,
    pub error_jobs: i64,
    pub last_error: Option<String>,
    pub hot_rows: i64,
}

async fn passive_koldstore_state(
    client: &tokio_postgres::Client,
    relation: &str,
) -> Result<PassiveKoldStoreState> {
    let row = client
        .query_one(
            r#"
            WITH worker AS (
                SELECT koldstore.async_mirror_status() AS status
            ),
            jobs AS (
                SELECT
                    count(*) FILTER (WHERE status = 'completed')::bigint AS completed_jobs,
                    count(*) FILTER (WHERE status IN ('pending', 'running'))::bigint AS active_jobs,
                    count(*) FILTER (WHERE status = 'error')::bigint AS error_jobs,
                    max(error_trace) FILTER (WHERE status = 'error') AS last_error
                FROM koldstore.jobs
                WHERE table_oid = $1::text::regclass::oid
                  AND job_type = 'flush'
                  AND created_at >= COALESCE(
                      (
                          SELECT created_at
                          FROM koldstore.schemas
                          WHERE table_oid = $1::text::regclass::oid AND active
                      ),
                      '-infinity'::timestamptz
                  )
            )
            SELECT
                (worker.status->'wal_applier'->>'pid')::integer,
                COALESCE((worker.status->'wal_applier'->>'required')::boolean, false),
                COALESCE((worker.status->'wal_applier'->>'pending')::boolean, false),
                COALESCE((worker.status->'wal_applier'->>'wal_generation')::bigint, 0),
                COALESCE((worker.status->'wal_applier'->>'wal_processed_generation')::bigint, 0),
                jobs.completed_jobs,
                jobs.active_jobs,
                jobs.error_jobs,
                jobs.last_error
            FROM worker CROSS JOIN jobs
            "#,
            &[&relation],
        )
        .await?;

    Ok(PassiveKoldStoreState {
        wal_pid: row.get(0),
        wal_required: row.get(1),
        wal_pending: row.get(2),
        wal_generation: row.get(3),
        wal_processed_generation: row.get(4),
        completed_jobs: row.get(5),
        active_jobs: row.get(6),
        error_jobs: row.get(7),
        last_error: row.get(8),
        hot_rows: super::sql::hot_row_count(client, relation).await?,
    })
}

/// Passively waits for WAL acknowledgement and automatic flush convergence.
///
/// This oracle must remain observational: it intentionally does not ensure,
/// fence, tick, flush, restart, or recover the subsystem under test.
///
/// # Errors
///
/// Returns an error when state probes fail or the deadline expires.
pub async fn wait_for_passive_convergence(
    client: &tokio_postgres::Client,
    relation: &str,
    target_lsn: &str,
    minimum_completed_jobs: i64,
    hot_row_limit: i64,
    deadline: Duration,
) -> Result<PassiveKoldStoreState> {
    let started = Instant::now();
    loop {
        let state = passive_koldstore_state(client, relation).await?;
        let progress = async_mirror_progress(client).await?;
        let slot_reached_target: bool = client
            .query_one(
                "SELECT $1::text::pg_lsn >= $2::text::pg_lsn",
                &[&progress.confirmed_flush_lsn, &target_lsn],
            )
            .await?
            .get(0);
        let wal_caught_up = state.wal_required
            && state.wal_pid.is_some_and(|pid| pid > 0)
            && !state.wal_pending
            && state.wal_generation == state.wal_processed_generation
            && slot_reached_target;
        // Slot/apply-lock finalize races are retryable under concurrent WAL apply:
        // a later completed job can still drain hot. Require no active work and
        // hot under limit; do not demand a historically empty error column.
        let flush_caught_up = state.completed_jobs >= minimum_completed_jobs
            && state.active_jobs == 0
            && state.hot_rows <= hot_row_limit
            && error_jobs_are_acceptable(&state);
        if wal_caught_up && flush_caught_up {
            return Ok(state);
        }
        if let Some(reason) = non_retryable_flush_failure(&state, hot_row_limit) {
            anyhow::bail!(
                "passive convergence saw a non-retryable flush failure; relation={relation}, \
                 target_lsn={target_lsn}, confirmed_flush_lsn={}, reason={reason}, state={state:?}",
                progress.confirmed_flush_lsn
            );
        }
        anyhow::ensure!(
            started.elapsed() <= deadline,
            "passive convergence timed out after {deadline:?}; relation={relation}, \
             target_lsn={target_lsn}, confirmed_flush_lsn={}, state={state:?}",
            progress.confirmed_flush_lsn
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Slot/apply-lock finalize failures are expected under concurrent apply and are
/// retried by a later auto-flush job. Other terminal errors are not acceptable
/// once the queue is idle and hot is still above the limit.
fn error_jobs_are_acceptable(state: &PassiveKoldStoreState) -> bool {
    if state.error_jobs == 0 {
        return true;
    }
    match state.last_error.as_deref() {
        Some(error) => is_retryable_flush_error(error),
        None => false,
    }
}

fn non_retryable_flush_failure(
    state: &PassiveKoldStoreState,
    hot_row_limit: i64,
) -> Option<&'static str> {
    if state.error_jobs == 0 || state.active_jobs != 0 || state.hot_rows <= hot_row_limit {
        return None;
    }
    match state.last_error.as_deref() {
        Some(error) if is_retryable_flush_error(error) => None,
        Some(_) => Some("terminal flush error with hot still above limit"),
        None => Some("error job without error_trace"),
    }
}

fn is_retryable_flush_error(error: &str) -> bool {
    error.contains("slot lock") || error.contains("apply lock")
}

/// Passively waits for the persistent WAL applier to appear.
///
/// # Errors
///
/// Returns an error when the status probe fails or the deadline expires.
pub async fn wait_for_wal_applier_passively(
    client: &tokio_postgres::Client,
    deadline: Duration,
) -> Result<i32> {
    let started = Instant::now();
    loop {
        let pid: Option<i32> = client
            .query_one(
                "SELECT (koldstore.async_mirror_status()->'wal_applier'->>'pid')::integer",
                &[],
            )
            .await?
            .get(0);
        if let Some(pid) = pid.filter(|pid| *pid > 0) {
            return Ok(pid);
        }
        anyhow::ensure!(
            started.elapsed() <= deadline,
            "WAL applier did not appear passively within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AsyncWorkerState {
    slot_present: bool,
    wal_registered: bool,
    wal_pid: Option<i32>,
    wal_running: bool,
    wal_starting: bool,
    wal_pending: bool,
    recovery_requested: bool,
    wal_generation: i64,
    wal_processed_generation: i64,
    maintenance_generation: i64,
    maintenance_processed_generation: i64,
}

impl AsyncWorkerState {
    fn settled(self) -> bool {
        !self.wal_pending
            && !self.recovery_requested
            && self.wal_generation == self.wal_processed_generation
            && self.maintenance_generation == self.maintenance_processed_generation
    }

    fn wal_available(self) -> bool {
        self.slot_present && self.wal_registered && self.wal_running
    }
}

async fn async_worker_state(client: &tokio_postgres::Client) -> Result<AsyncWorkerState> {
    let row = client
        .query_one(
            r#"
            SELECT
              COALESCE((status->'slot'->>'present')::boolean, false),
              COALESCE((status->'wal_applier'->>'registered')::boolean, false),
              (status->'wal_applier'->>'pid')::integer,
              COALESCE((status->'wal_applier'->>'running')::boolean, false),
              COALESCE((status->'wal_applier'->>'starting')::boolean, false),
              COALESCE((status->'wal_applier'->>'pending')::boolean, false),
              COALESCE((status->'maintenance'->>'recovery_requested')::boolean, false),
              COALESCE((status->'wal_applier'->>'wal_generation')::bigint, 0),
              COALESCE((status->'wal_applier'->>'wal_processed_generation')::bigint, 0),
              COALESCE((status->'maintenance'->>'maintenance_generation')::bigint, 0),
              COALESCE((status->'maintenance'->>'maintenance_processed_generation')::bigint, 0)
            FROM (SELECT koldstore.async_mirror_status() AS status) s
            "#,
            &[],
        )
        .await?;
    Ok(AsyncWorkerState {
        slot_present: row.get(0),
        wal_registered: row.get(1),
        wal_pid: row.get(2),
        wal_running: row.get(3),
        wal_starting: row.get(4),
        wal_pending: row.get(5),
        recovery_requested: row.get(6),
        wal_generation: row.get(7),
        wal_processed_generation: row.get(8),
        maintenance_generation: row.get(9),
        maintenance_processed_generation: row.get(10),
    })
}

/// Requests async capture and waits until the persistent database WAL applier is
/// running or starting.
///
/// Before a test creates its first managed table there may be no logical slot;
/// that state is already idle and needs no process. Once a slot exists, the
/// helper requires the persistent service rather than accepting a caught-up but
/// absent worker.
///
/// # Errors
///
/// Returns an error when the request fails or the supervisor does not publish a
/// persistent process within [`WORKER_START_DEADLINE`].
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
        let state = async_worker_state(client).await?;
        if !state.slot_present || state.wal_available() {
            return Ok(started.elapsed());
        }
        anyhow::ensure!(
            started.elapsed() <= WORKER_START_DEADLINE,
            "persistent WAL applier was not acknowledged within {WORKER_START_DEADLINE:?}; state={state:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Waits for supervisor-owned recovery after the persistent WAL applier was
/// killed.
///
/// This deliberately does not call `internal_ensure_async_mirror_worker`: the
/// child lifecycle signal / safety reconciliation must replace the process on
/// its own even when the database is already caught up. The replacement must
/// publish a different live PID than `previous_pid`.
///
/// # Errors
///
/// Returns an error when supervisor recovery does not settle before the deadline.
pub async fn wait_for_async_worker_auto_restart(
    client: &tokio_postgres::Client,
    previous_pid: i32,
) -> Result<Duration> {
    let started = Instant::now();
    loop {
        let state = async_worker_state(client).await?;
        if state.wal_available() && state.wal_pid.is_some_and(|pid| pid != previous_pid) {
            return Ok(started.elapsed());
        }
        anyhow::ensure!(
            started.elapsed() <= WORKER_START_DEADLINE,
            "persistent WAL applier did not recover within {WORKER_START_DEADLINE:?}; state={state:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Returns whether the persistent WAL-applier service is running or starting for
/// the current database.
///
/// # Errors
///
/// Returns an error when the async status probe fails.
pub async fn async_worker_running(client: &tokio_postgres::Client) -> Result<bool> {
    Ok(async_worker_state(client).await?.wal_available())
}

async fn wal_process_running(client: &tokio_postgres::Client) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT EXISTS (\
               SELECT 1 FROM pg_catalog.pg_stat_activity \
               WHERE backend_type = 'koldstore wal applier ' \
                 || (SELECT oid::text FROM pg_catalog.pg_database \
                     WHERE datname = current_database())\
             )",
            &[],
        )
        .await?
        .get(0))
}

async fn wal_process_pid(client: &tokio_postgres::Client) -> Result<Option<i32>> {
    if let Some(pid) = async_worker_state(client).await?.wal_pid {
        return Ok(Some(pid));
    }
    Ok(client
        .query_one(
            "SELECT (\
               SELECT pid::integer \
               FROM pg_catalog.pg_stat_activity \
               WHERE backend_type = 'koldstore wal applier ' \
                 || (SELECT oid::text FROM pg_catalog.pg_database \
                     WHERE datname = current_database()) \
               LIMIT 1\
             )",
            &[],
        )
        .await?
        .get(0))
}

async fn terminate_pid(client: &tokio_postgres::Client, pid: i32) -> Result<bool> {
    Ok(client
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await?
        .get(0))
}

async fn pid_is_running(client: &tokio_postgres::Client, pid: i32) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = $1)",
            &[&pid],
        )
        .await?
        .get(0))
}

/// Terminates the persistent WAL applier for the current database.
///
/// The helper waits for the original PID to disappear, not for the backend type
/// to become absent: an unpaused supervisor may replace the process immediately.
/// When no process is visible, one diagnostic request is published and observed
/// briefly before reporting that no worker could be signalled.
///
/// # Errors
///
/// Returns an error when termination SQL or worker status probing fails.
pub async fn terminate_async_worker(client: &tokio_postgres::Client) -> Result<bool> {
    let mut target_pid = wal_process_pid(client).await?;
    if target_pid.is_none() {
        let requested: bool = client
            .query_one(
                "SELECT koldstore.internal_ensure_async_mirror_worker()",
                &[],
            )
            .await?
            .get(0);
        if !requested {
            return Ok(false);
        }
        let observe_started = Instant::now();
        while observe_started.elapsed() <= WORKER_OBSERVE_DEADLINE {
            target_pid = wal_process_pid(client).await?;
            if target_pid.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    let Some(pid) = target_pid else {
        return Ok(false);
    };
    let _ = terminate_pid(client, pid).await?;
    let started = Instant::now();
    while pid_is_running(client, pid).await? {
        anyhow::ensure!(
            started.elapsed() <= WORKER_START_DEADLINE,
            "WAL applier pid={pid} did not exit within {WORKER_START_DEADLINE:?} after terminate"
        );
        let _ = terminate_pid(client, pid).await?;
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

/// Pauses supervisor WAL/maintenance dispatch and terminates the live WAL
/// applier.
///
/// PostgreSQL advisory locks are database-local, so the test control uses the
/// extension's shared-memory pause set. Call [`wait_for_async_worker`] or
/// [`release_async_worker_stop_lock`] when dispatch should resume.
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
        if !wal_process_running(client).await? {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "persistent WAL applier did not stay stopped within 10s"
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

/// Passively waits until autonomous WAL application has produced the expected
/// mirror row count.
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
        let state = async_worker_state(client).await?;
        let settled = state.settled();
        anyhow::ensure!(
            started.elapsed() <= BACKGROUND_APPLY_DEADLINE,
            "timed out after {BACKGROUND_APPLY_DEADLINE:?} waiting for {expected} mirror rows with op={op}; actual={actual}, settled={settled}, workers={state:?}"
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
