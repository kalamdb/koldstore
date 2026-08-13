//! One-shot flush executor background workers.
//!
//! Queue callers never register workers directly. They commit durable jobs and
//! publish a queue generation; the cluster supervisor owns worker registration
//! and capacity. Each executor tries a bounded fair page, claims one lockable
//! table, runs one job, then exits.

use std::time::Duration;

use koldstore_common::unix_now_ms;
use koldstore_flush::{
    plan_next_pending_flush_due_epoch_ms, plan_select_pending_flush_candidates,
    plan_select_pending_flush_candidates_after,
};
use koldstore_supervisor::{flush_executor_worker_type, DatabaseOid, LIBRARY_NAME};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder};
use pgrx::datum::DatumWithOid;

use super::txn;

const FLUSH_EXECUTOR_FUNCTION: &str = "koldstore_flush_executor_main";
const CANDIDATE_PAGE_SIZE: i64 = 16;
const BUSY_RETRY: Duration = Duration::from_millis(200);

/// Reconstructs queue dispatch hints after postmaster/worker recovery.
/// Must run inside a database-local maintenance transaction.
pub(crate) fn reconcile_queue_after_recovery(database_oid: u32) -> Result<(), String> {
    if crate::sql::flush::jobs::has_due_pending_flush_jobs().map_err(|e| e.to_string())? {
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
    cursor: PendingCandidateCursor,
}

#[derive(Debug, Clone, Copy)]
struct PendingCandidateCursor {
    available_at: pgrx::datum::TimestampWithTimeZone,
    updated_at: pgrx::datum::TimestampWithTimeZone,
    id: pgrx::Uuid,
}

fn pending_candidates(
    after: Option<PendingCandidateCursor>,
) -> Result<Vec<PendingCandidate>, String> {
    let statement = match after {
        Some(_) => {
            plan_select_pending_flush_candidates_after().map_err(|error| error.to_string())?
        }
        None => plan_select_pending_flush_candidates().map_err(|error| error.to_string())?,
    };
    let mut args = vec![DatumWithOid::from(CANDIDATE_PAGE_SIZE)];
    if let Some(after) = after {
        args.extend([
            DatumWithOid::from(after.available_at),
            DatumWithOid::from(after.updated_at),
            DatumWithOid::from(after.id),
        ]);
    }
    crate::spi::execute_prepared(&statement, &args, |table| {
        let mut candidates = Vec::with_capacity(CANDIDATE_PAGE_SIZE as usize);
        for row in table {
            let table_oid = row
                .get::<pgrx::pg_sys::Oid>(1)?
                .ok_or_else(|| crate::spi::missing_attribute("table_oid"))?;
            let force = row.get::<bool>(2)?.unwrap_or(false);
            let available_at = row
                .get::<pgrx::datum::TimestampWithTimeZone>(3)?
                .ok_or_else(|| crate::spi::missing_attribute("available_at"))?;
            let updated_at = row
                .get::<pgrx::datum::TimestampWithTimeZone>(4)?
                .ok_or_else(|| crate::spi::missing_attribute("updated_at"))?;
            let id = row
                .get::<pgrx::Uuid>(5)?
                .ok_or_else(|| crate::spi::missing_attribute("id"))?;
            candidates.push(PendingCandidate {
                table_oid,
                force,
                cursor: PendingCandidateCursor {
                    available_at,
                    updated_at,
                    id,
                },
            });
        }
        Ok(candidates)
    })
    .map_err(|error| error.to_string())
}

fn next_pending_due_ms() -> Result<Option<i64>, String> {
    let statement = plan_next_pending_flush_due_epoch_ms().map_err(|error| error.to_string())?;
    pgrx::Spi::get_one::<i64>(&statement.sql).map_err(|error| error.to_string())
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
    let mut after = None;
    let mut saw_candidate = false;

    loop {
        let candidates = pending_candidates(after)?;
        if candidates.is_empty() {
            return Ok((
                if saw_candidate {
                    ClaimOutcome::Busy
                } else {
                    ClaimOutcome::Empty
                },
                None,
            ));
        }
        saw_candidate = true;
        after = candidates.last().map(|candidate| candidate.cursor);

        for candidate in candidates {
            let Some(guard) =
                crate::sql::job_lock::TableJobLockGuard::try_lock(candidate.table_oid)?
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
                    pgrx::log!(
                        "koldstore flush executor: candidate table_oid={} changed before claim: {error}",
                        candidate.table_oid.to_u32()
                    );
                    drop(guard);
                }
            }
        }
    }
}

struct FlushWorkerRegistration {
    database_oid: u32,
    queue_generation: u64,
}

impl FlushWorkerRegistration {
    fn start(database_oid: u32) -> Option<Self> {
        let effective_limit = u32::try_from(crate::guc::max_parallel_flush_jobs())
            .unwrap_or(1)
            .max(1);
        if !super::wake::flush_started(database_oid, effective_limit) {
            return None;
        }
        let queue_generation = super::wake::supervisor_snapshot(database_oid)
            .map(|snapshot| snapshot.flush_generation)
            .unwrap_or(0);
        Some(Self {
            database_oid,
            queue_generation,
        })
    }

    fn reconcile_queue(&self, outcome: ClaimOutcome) {
        let next_due = match outcome {
            ClaimOutcome::Busy => Some(
                unix_now_ms().saturating_add(i64::try_from(BUSY_RETRY.as_millis()).unwrap_or(200)),
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
    let Some(registration) = FlushWorkerRegistration::start(database_oid) else {
        pgrx::log!(
            "koldstore flush executor db={database_oid}: stale/unreserved start; exiting before queue access"
        );
        return;
    };

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

    registration.reconcile_queue(ClaimOutcome::Claimed);
}
