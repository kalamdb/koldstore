//! One-shot flush executor background workers.
//!
//! The database coordinator (and `flush_table` in queue mode) spawns at most
//! `koldstore.max_parallel_flush_jobs` of these workers. Each worker claims one
//! pending flush job under session table ownership, runs it, then exits so
//! PostgreSQL releases the session advisory lock automatically.
//!
//! Claim commits separately from encode/upload/finalize so a crash after claim
//! leaves a resumable `running` job rather than an invisible in-flight attempt.

use koldstore_worker::{flush_executor_worker_type, DatabaseOid, LIBRARY_NAME};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder};
use pgrx::datum::DatumWithOid;

const FLUSH_EXECUTOR_FUNCTION: &str = "koldstore_flush_executor_main";

/// Counts live flush executor backends for `database_oid`.
fn flush_executor_count(worker_type: &str) -> Result<i64, String> {
    pgrx::Spi::get_one_with_args::<i64>(
        "SELECT count(*)::bigint FROM pg_catalog.pg_stat_activity WHERE backend_type = $1",
        &[DatumWithOid::from(worker_type)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "flush executor activity query returned no row".to_string())
}

/// Spawns one flush executor when under the parallel cap and pending work exists.
///
/// Only waits for postmaster startup notification (same deadlock avoidance as
/// async-mirror ensure: the worker cannot finish connecting until this
/// transaction commits).
///
/// # Errors
///
/// Returns an error when activity probes or dynamic registration fail.
pub(crate) fn spawn_flush_executor_if_needed() -> Result<bool, String> {
    let pending = crate::sql::flush::jobs::count_pending_flush_jobs().map_err(|e| e.to_string())?;
    if pending <= 0 {
        return Ok(false);
    }
    spawn_flush_executors_upto(1).map(|spawned| spawned > 0)
}

/// Spawns flush executors until `max_parallel_flush_jobs` or pending work is met.
///
/// # Errors
///
/// Returns an error when activity probes or dynamic registration fail.
pub(crate) fn spawn_flush_executors_for_pending_work() -> Result<u32, String> {
    let pending = crate::sql::flush::jobs::count_pending_flush_jobs().map_err(|e| e.to_string())?;
    if pending <= 0 {
        return Ok(0);
    }
    let max = i64::from(crate::guc::max_parallel_flush_jobs());
    let to_spawn = pending.min(max).max(0);
    let Ok(to_spawn) = u32::try_from(to_spawn) else {
        return Ok(0);
    };
    spawn_flush_executors_upto(to_spawn)
}

fn spawn_flush_executors_upto(limit: u32) -> Result<u32, String> {
    if limit == 0 {
        return Ok(0);
    }
    let database_oid = DatabaseOid::new(unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32());
    let worker_type = flush_executor_worker_type(database_oid);
    let max = i64::from(crate::guc::max_parallel_flush_jobs());
    let running = flush_executor_count(&worker_type)?;
    let available = u32::try_from((max - running).max(0)).unwrap_or(0);
    let to_spawn = limit.min(available);
    let mut spawned = 0_u32;
    while spawned < to_spawn {
        if !register_one_flush_executor(database_oid, &worker_type)? {
            break;
        }
        spawned = spawned.saturating_add(1);
    }
    Ok(spawned)
}

fn register_one_flush_executor(
    database_oid: DatabaseOid,
    worker_type: &str,
) -> Result<bool, String> {
    // Dynamic NEVER_RESTART workers: crash recovery is a new spawn from the
    // coordinator or the next flush_table call.
    let worker = BackgroundWorkerBuilder::new(worker_type)
        .set_type(worker_type)
        .set_library(LIBRARY_NAME)
        .set_function(FLUSH_EXECUTOR_FUNCTION)
        .enable_spi_access()
        .set_restart_time(None)
        .set_argument(Some(pgrx::pg_sys::Datum::from(database_oid.get())))
        .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
        .load_dynamic()
        .map_err(|_| {
            format!(
                "could not register flush executor \
                 (worker_type={worker_type}; usually max_worker_processes exhausted)"
            )
        })?;
    worker
        .wait_for_startup()
        .map_err(|status| format!("flush executor did not start: {status:?}"))?;
    Ok(true)
}

fn worker_transaction<R>(body: impl FnOnce() -> Result<R, String>) -> Result<R, String> {
    unsafe {
        pgrx::pg_sys::SetCurrentStatementStartTimestamp();
        pgrx::pg_sys::StartTransactionCommand();
        pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot());
    }
    let result = body();
    unsafe {
        if result.is_ok() && !pgrx::pg_sys::IsAbortedTransactionBlockState() {
            pgrx::pg_sys::PopActiveSnapshot();
            pgrx::pg_sys::CommitTransactionCommand();
        } else {
            pgrx::pg_sys::AbortCurrentTransaction();
        }
    }
    result
}

/// Claim outcome that keeps session ownership across the claim→work commit.
struct ClaimedWork {
    table_oid: pgrx::pg_sys::Oid,
    guard: crate::sql::job_lock::TableJobLockGuard,
    claimed: crate::sql::flush::execute::ClaimedFlushJob,
}

fn claim_one_flush_job() -> Result<Option<ClaimedWork>, String> {
    let reclaimed = crate::sql::flush::jobs::reclaim_orphan_running_flush_jobs()
        .map_err(|error| error.to_string())?;
    if reclaimed > 0 {
        pgrx::log!("koldstore flush executor: reclaimed {reclaimed} stuck running flush job(s)");
    }

    let Some((table_oid, force)) = crate::sql::flush::jobs::select_pending_flush_candidate()
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    let table_oid = pgrx::pg_sys::Oid::from(table_oid.get());

    let Some(guard) = crate::sql::job_lock::TableJobLockGuard::try_lock(table_oid)? else {
        pgrx::log!(
            "koldstore flush executor: skipping table_oid={} (lock busy)",
            table_oid.to_u32()
        );
        return Ok(None);
    };

    let claimed = crate::sql::flush::execute::claim_flush_job_for_executor(table_oid, force)?;
    Ok(Some(ClaimedWork {
        table_oid,
        guard,
        claimed,
    }))
}

/// One-shot flush executor entry point (`NEVER_RESTART`).
///
/// SQL / C contract: `koldstore_flush_executor_main(database_oid)`.
#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_flush_executor_main(argument: pgrx::pg_sys::Datum) {
    let database_oid = argument.value() as u32;
    BackgroundWorker::connect_worker_to_spi_by_oid(
        Some(pgrx::pg_sys::Oid::from(database_oid)),
        None,
    );

    // Short claim transaction: durable running + attempt_token before any I/O.
    let claimed = match worker_transaction(claim_one_flush_job) {
        Ok(claimed) => claimed,
        Err(error) => {
            pgrx::warning!("koldstore flush executor claim failed: {error}");
            return;
        }
    };
    let Some(ClaimedWork {
        table_oid,
        guard,
        claimed,
    }) = claimed
    else {
        return;
    };

    // Second transaction: encode/upload without slot lock; finalize try-locks slot.
    if let Err(error) = worker_transaction(|| {
        crate::sql::flush::execute::run_claimed_flush_with_session_lock(table_oid, guard, claimed)
            .map(|_| ())
    }) {
        pgrx::warning!("koldstore flush executor failed: {error}");
    }
}
