//! One-shot flush executor background workers.
//!
//! Queue callers never register workers directly. They commit durable jobs and
//! publish a queue generation; the single cluster supervisor owns dynamic
//! worker registration and shared Starting/Running reservations. Each executor
//! claims one table under the existing session advisory lock, runs one job, and
//! exits so PostgreSQL releases all session ownership automatically.

use koldstore_worker::{flush_executor_worker_type, DatabaseOid, LIBRARY_NAME};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder};

use super::txn;

const FLUSH_EXECUTOR_FUNCTION: &str = "koldstore_flush_executor_main";

/// Compatibility entry point used by queue callers while call sites migrate.
///
/// IMPORTANT: this function no longer registers a background worker. It merely
/// records a post-commit queue wake. The supervisor is the only production code
/// allowed to call [`register_flush_executor_from_supervisor`].
pub(crate) fn spawn_flush_executor_if_needed() -> Result<bool, String> {
    let pending = crate::sql::flush::jobs::count_pending_flush_jobs().map_err(|e| e.to_string())?;
    if pending <= 0 {
        return Ok(false);
    }
    super::wake::mark_flush_queue_pending();
    Ok(true)
}

/// Compatibility entry point for the old scheduler.
///
/// It now publishes one queue wake rather than creating N workers itself.
/// Capacity and fan-out belong to the supervisor.
pub(crate) fn spawn_flush_executors_for_pending_work() -> Result<u32, String> {
    let pending = crate::sql::flush::jobs::count_pending_flush_jobs().map_err(|e| e.to_string())?;
    if pending <= 0 {
        return Ok(0);
    }
    super::wake::mark_flush_queue_pending();
    Ok(1)
}

/// Registers one already-reserved flush executor.
///
/// This is called only by the static cluster supervisor. The shared reservation
/// must be acquired before this function so STARTING workers count toward all
/// capacity limits. Registration is intentionally non-blocking: the worker
/// changes Starting -> Running after it connects.
pub(crate) fn register_flush_executor_from_supervisor(database_oid: u32) -> Result<(), String> {
    let database_oid = DatabaseOid::new(database_oid);
    let worker_type = flush_executor_worker_type(database_oid);
    BackgroundWorkerBuilder::new(&worker_type)
        .set_type(&worker_type)
        .set_library(LIBRARY_NAME)
        .set_function(FLUSH_EXECUTOR_FUNCTION)
        .enable_spi_access()
        .set_restart_time(None)
        .set_argument(Some(pgrx::pg_sys::Datum::from(database_oid.get())))
        // PostgreSQL notifies the supervisor process when this child starts or
        // exits; the worker also best-effort wakes the current supervisor on
        // normal exit so supervisor replacement is safe.
        .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
        .load_dynamic()
        .map(|_| ())
        .map_err(|_| {
            format!(
                "could not register flush executor (worker_type={worker_type}; \
                 usually max_worker_processes exhausted)"
            )
        })
}

/// Claim outcome that keeps session ownership across the claim -> work commit.
struct ClaimedWork {
    table_oid: pgrx::pg_sys::Oid,
    guard: crate::sql::job_lock::TableJobLockGuard,
    claimed: crate::sql::flush::execute::ClaimedFlushJob,
}

fn claim_one_flush_job() -> Result<Option<ClaimedWork>, String> {
    // Orphan recovery belongs to the database maintenance/recovery worker, not
    // every heavy executor. An executor should only inspect runnable queue work.
    let Some((table_oid, force)) = crate::sql::flush::jobs::select_pending_flush_candidate()
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    let table_oid = pgrx::pg_sys::Oid::from(table_oid.get());

    let Some(guard) = crate::sql::job_lock::TableJobLockGuard::try_lock(table_oid)? else {
        // Leave the queue generation dirty. The supervisor/recovery pass will
        // retry without converting normal table contention into a terminal job.
        pgrx::log!(
            "koldstore flush executor: table_oid={} busy; yielding queue ownership",
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

struct FlushWorkerRegistration {
    database_oid: u32,
    queue_generation: u64,
}

impl FlushWorkerRegistration {
    fn start(database_oid: u32) -> Self {
        let effective_limit = u32::try_from(crate::guc::max_parallel_flush_jobs())
            .unwrap_or(1)
            .max(1);
        super::wake::flush_started(database_oid, effective_limit);
        let queue_generation = super::wake::supervisor_snapshot(database_oid)
            .map(|snapshot| snapshot.flush_generation)
            .unwrap_or(0);
        Self {
            database_oid,
            queue_generation,
        }
    }

    fn mark_drained_if_empty(&self) {
        let empty = txn::run(|| {
            crate::sql::flush::jobs::count_pending_flush_jobs()
                .map(|count| count <= 0)
                .map_err(|error| error.to_string())
        })
        .unwrap_or(false);
        if empty {
            super::wake::mark_flush_processed(self.database_oid, self.queue_generation);
        }
    }
}

impl Drop for FlushWorkerRegistration {
    fn drop(&mut self) {
        self.mark_drained_if_empty();
        super::wake::flush_stopped(self.database_oid);
    }
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
    let _registration = FlushWorkerRegistration::start(database_oid);

    // Short claim transaction: durable running + attempt_token before any I/O.
    let claimed = match txn::run(claim_one_flush_job) {
        Ok(claimed) => claimed,
        Err(error) => {
            pgrx::warning!("koldstore flush executor claim failed: {error}");
            super::wake::request_recovery(database_oid);
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

    // Short-txn flush path: upload outside Postgres txns; catalog SPI uses
    // FlushCommitStyle::Short. Do not wrap the entire flush in one transaction.
    if let Err(error) =
        crate::sql::flush::execute::run_claimed_flush_with_session_lock(table_oid, guard, claimed)
    {
        pgrx::warning!("koldstore flush executor failed: {error}");
        super::wake::request_recovery(database_oid);
    }
}
