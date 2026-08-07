//! One-shot flush executor background workers.
//!
//! Queue callers never register workers directly. They commit durable jobs and
//! publish a queue generation; the cluster supervisor owns worker registration
//! and capacity. Each executor tries a bounded fair page, claims one lockable
//! table, runs one job, then exits.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use koldstore_worker::{flush_executor_worker_type, DatabaseOid, LIBRARY_NAME};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder};
use pgrx::datum::DatumWithOid;

use super::txn;

const FLUSH_EXECUTOR_FUNCTION: &str = "koldstore_flush_executor_main";
const CANDIDATE_PAGE_SIZE: i64 = 16;
const BUSY_RETRY: Duration = Duration::from_millis(200);

/// Compatibility entry point used by queue callers while call sites migrate.
/// It publishes a post-commit queue wake; it never registers a process itself.
pub(crate) fn spawn_flush_executor_if_needed() -> Result<bool, String> {
    let pending = crate::sql::flush::jobs::count_pending_flush_jobs().map_err(|e| e.to_string())?;
    if pending <= 0 {
        return Ok(false);
    }
    super::wake::mark_flush_queue_pending();
    Ok(true)
}

/// Compatibility entry point for the scheduler. Capacity/fan-out belong to the
/// supervisor; this function only publishes one coalesced queue event.
pub(crate) fn spawn_flush_executors_for_pending_work() -> Result<u32, String> {
    let pending = crate::sql::flush::jobs::count_pending_flush_jobs().map_err(|e| e.to_string())?;
    if pending <= 0 {
        return Ok(0);
    }
    super::wake::mark_flush_queue_pending();
    Ok(1)
}

/// Reconstructs queue dispatch hints after postmaster/worker recovery.
/// Must run inside a database-local maintenance transaction.
pub(crate) fn reconcile_queue_after_recovery(database_oid: u32) -> Result<(), String> {
    let due = crate::sql::flush::jobs::count_pending_flush_jobs().map_err(|e| e.to_string())?;
    if due > 0 {
        super::wake::mark_flush_queue_pending();
        return Ok(());
    }
    match next_pending_due_ms()? {
        Some(deadline_ms) if deadline_ms > 0 => {
            super::wake::schedule_flush_at_ms(database_oid, deadline_ms);
        }
        _ => super::wake::clear_flush_deadline(database_oid),
    }
    Ok(())
}

/// Registers one already-reserved flush executor. Called only by the supervisor.
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
        // PostgreSQL notifies the registering supervisor on child start/exit.
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

#[derive(Debug, Clone, Copy)]
struct PendingCandidate {
    table_oid: pgrx::pg_sys::Oid,
    force: bool,
}

/// Reads a small fair page. Table locking happens after this query, so a busy
/// first table cannot head-of-line block unrelated flush work.
fn pending_candidates() -> Result<Vec<PendingCandidate>, String> {
    pgrx::Spi::connect(|client| {
        let table = client
            .select(
                "SELECT table_oid::oid, COALESCE((payload->>'force')::boolean, false) \
                 FROM koldstore.jobs \
                 WHERE job_type = 'flush' \
                   AND status = 'pending' \
                   AND available_at <= clock_timestamp() \
                 ORDER BY available_at, updated_at, id \
                 LIMIT $1",
                Some(1),
                &[DatumWithOid::from(CANDIDATE_PAGE_SIZE)],
            )
            .map_err(|error| error.to_string())?;
        let mut candidates = Vec::new();
        for row in table {
            let table_oid = row
                .get::<pgrx::pg_sys::Oid>(1)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "pending flush candidate missing table_oid".to_string())?;
            let force = row
                .get::<bool>(2)
                .map_err(|error| error.to_string())?
                .unwrap_or(false);
            candidates.push(PendingCandidate { table_oid, force });
        }
        Ok(candidates)
    })
}

/// Earliest pending `available_at`, including jobs that are not due yet.
fn next_pending_due_ms() -> Result<Option<i64>, String> {
    pgrx::Spi::get_one::<i64>(
        "SELECT (extract(epoch FROM min(available_at)) * 1000)::bigint \
         FROM koldstore.jobs \
         WHERE job_type = 'flush' AND status = 'pending'",
    )
    .map_err(|error| error.to_string())
}

struct ClaimedWork {
    table_oid: pgrx::pg_sys::Oid,
    guard: crate::sql::job_lock::TableJobLockGuard,
    claimed: crate::sql::flush::execute::ClaimedFlushJob,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimOutcome {
    Claimed,
    Empty,
    Busy,
}

fn claim_one_flush_job() -> Result<(ClaimOutcome, Option<ClaimedWork>), String> {
    let candidates = pending_candidates()?;
    if candidates.is_empty() {
        return Ok((ClaimOutcome::Empty, None));
    }

    for candidate in candidates {
        let Some(guard) = crate::sql::job_lock::TableJobLockGuard::try_lock(candidate.table_oid)?
        else {
            continue;
        };
        match crate::sql::flush::execute::claim_flush_job_for_executor(
            candidate.table_oid,
            candidate.force,
        ) {
            Ok(claimed) => {
                return Ok((
                    ClaimOutcome::Claimed,
                    Some(ClaimedWork {
                        table_oid: candidate.table_oid,
                        guard,
                        claimed,
                    }),
                ));
            }
            Err(error) => {
                // Candidate state may change between page read and exact claim.
                pgrx::log!(
                    "koldstore flush executor: candidate table_oid={} changed before claim: {error}",
                    candidate.table_oid.to_u32()
                );
                drop(guard);
            }
        }
    }

    Ok((ClaimOutcome::Busy, None))
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

    /// Reconciles the queue after a no-claim or completed attempt. It
    /// acknowledges only the generation this worker started for; a concurrent
    /// enqueue advances the generation and therefore cannot be cleared here.
    fn reconcile_queue(&self, outcome: ClaimOutcome) {
        let next_due = match outcome {
            ClaimOutcome::Busy => Some(
                unix_now_ms()
                    .saturating_add(i64::try_from(BUSY_RETRY.as_millis()).unwrap_or(200)),
            ),
            ClaimOutcome::Empty | ClaimOutcome::Claimed => {
                txn::run(next_pending_due_ms).unwrap_or(None)
            }
        };

        let Some(snapshot) = super::wake::supervisor_snapshot(self.database_oid) else {
            return;
        };
        if snapshot.flush_generation != self.queue_generation {
            return;
        }

        if let Some(deadline_ms) = next_due.filter(|deadline| *deadline > 0) {
            super::wake::schedule_flush_at_ms(self.database_oid, deadline_ms);
        } else {
            super::wake::clear_flush_deadline(self.database_oid);
        }
        super::wake::mark_flush_processed(self.database_oid, self.queue_generation);
    }
}

impl Drop for FlushWorkerRegistration {
    fn drop(&mut self) {
        super::wake::flush_stopped(self.database_oid);
    }
}

#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_flush_executor_main(argument: pgrx::pg_sys::Datum) {
    let database_oid = argument.value() as u32;
    BackgroundWorker::connect_worker_to_spi_by_oid(
        Some(pgrx::pg_sys::Oid::from(database_oid)),
        None,
    );
    let registration = FlushWorkerRegistration::start(database_oid);

    let (claim_outcome, claimed) = match txn::run(claim_one_flush_job) {
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
        registration.reconcile_queue(claim_outcome);
        return;
    };

    if let Err(error) =
        crate::sql::flush::execute::run_claimed_flush_with_session_lock(table_oid, guard, claimed)
    {
        pgrx::warning!("koldstore flush executor failed: {error}");
        super::wake::request_recovery(database_oid);
        return;
    }

    // Avoid spawning an extra no-op executor merely to acknowledge a drained
    // queue. A concurrent enqueue remains protected by the generation check.
    registration.reconcile_queue(ClaimOutcome::Claimed);
}

fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
