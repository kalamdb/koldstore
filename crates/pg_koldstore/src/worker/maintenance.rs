//! Ephemeral database-maintenance background worker.
//!
//! The cluster supervisor owns registration. At most one maintenance worker is
//! active per database. It performs database-local recovery, policy scheduling,
//! and queue reconciliation, waits briefly to coalesce a maintenance burst, and
//! exits when those generations are caught up. Near-realtime WAL application is
//! owned by the separate persistent WAL applier.

use std::time::Duration;

use koldstore_worker::{maintenance_worker_type, DatabaseOid, LIBRARY_NAME};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};

const MAINTENANCE_FUNCTION: &str = "koldstore_maintenance_worker_main";
const IDLE_GRACE: Duration = Duration::from_millis(200);

/// Registers one already-reserved maintenance worker without waiting for startup.
///
/// Only the static cluster supervisor may call this in production. Shared state
/// reserves the database before registration so a second worker cannot race in
/// while PostgreSQL is still starting the first process.
pub(crate) fn register_maintenance_from_supervisor(database_oid: u32) -> Result<(), String> {
    let database_oid = DatabaseOid::new(database_oid);
    let worker_type = maintenance_worker_type(database_oid);
    BackgroundWorkerBuilder::new(&worker_type)
        .set_type(&worker_type)
        .set_library(LIBRARY_NAME)
        .set_function(MAINTENANCE_FUNCTION)
        .enable_spi_access()
        .set_restart_time(None)
        .set_argument(Some(pgrx::pg_sys::Datum::from(database_oid.get())))
        .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
        .load_dynamic()
        .map(|_| ())
        .map_err(|_| {
            format!(
                "could not register database maintenance worker \
                 (worker_type={worker_type}; usually max_worker_processes exhausted)"
            )
        })
}

/// Dynamic-worker C entry point registered by the cluster supervisor.
#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_maintenance_worker_main(argument: pgrx::pg_sys::Datum) {
    run_maintenance_worker(argument.value() as u32);
}

fn run_maintenance_worker(database_oid: u32) {
    attach_signal_handlers();
    BackgroundWorker::connect_worker_to_spi_by_oid(
        Some(pgrx::pg_sys::Oid::from(database_oid)),
        None,
    );

    if !super::wake::maintenance_started(database_oid) {
        pgrx::log!(
            "koldstore maintenance worker db={database_oid}: stale/unreserved start; exiting"
        );
        return;
    }
    let _registration = MaintenanceRegistration { database_oid };
    super::wake::set_flush_limit(
        database_oid,
        u32::try_from(crate::guc::max_parallel_flush_jobs())
            .unwrap_or(1)
            .max(1),
    );

    loop {
        let Some(snapshot) = super::wake::supervisor_snapshot(database_oid) else {
            return;
        };
        let target_maintenance_generation = snapshot.maintenance_generation;
        let recovery_requested =
            snapshot.event_flags & koldstore_worker::EVENT_RECOVERY_REQUIRED != 0;
        let schedule_requested = snapshot.event_flags & koldstore_worker::EVENT_SCHEDULE_DIRTY != 0;
        let needs_reconciliation = recovery_requested || schedule_requested;

        if needs_reconciliation {
            let maintenance_result = super::txn::run_recoverable("maintenance worker", || {
                if recovery_requested {
                    // A SIGKILL/FATAL executor cannot run Rust Drop. The supervisor
                    // marks RECOVERY_REQUIRED after native lifecycle reconciliation;
                    // reclaim durable owners before redispatch.
                    let reclaimed = crate::sql::flush::jobs::reclaim_orphan_running_flush_jobs()
                        .map_err(|error| error.to_string())?;
                    if reclaimed > 0 {
                        pgrx::log!(
                            "koldstore maintenance worker db={database_oid}: reclaimed {reclaimed} orphan flush job(s)"
                        );
                        super::wake::mark_flush_queue_pending();
                    }
                    super::flush_executor::reconcile_queue_after_recovery(database_oid)?;
                }

                super::flush_task::run_flush_scheduler_tick()
            });

            match maintenance_result {
                Ok(result) => {
                    update_timed_policy_deadline(database_oid, result.next_timed_wake_at_ms);
                    super::wake::mark_maintenance_reconciled(
                        database_oid,
                        target_maintenance_generation,
                    );
                }
                Err(error) => {
                    pgrx::warning!(
                        "koldstore maintenance worker db={database_oid} scheduling/recovery deferred: {error}"
                    );
                    super::wake::request_recovery(database_oid);
                    return;
                }
            }
        }

        if maintenance_due(database_oid) {
            continue;
        }

        // Brief interruptible grace amortizes fork/exit cost across a burst of
        // policy/recovery requests without making maintenance permanently resident.
        if !BackgroundWorker::wait_latch(Some(IDLE_GRACE)) {
            return;
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
        if !maintenance_due(database_oid) {
            return;
        }
    }
}

fn maintenance_due(database_oid: u32) -> bool {
    super::wake::supervisor_snapshot(database_oid).is_some_and(|state| {
        state.maintenance_generation != state.maintenance_processed_generation
    })
}

/// Replaces database timed-policy state after a full configuration/recovery
/// reconciliation.
fn update_timed_policy_deadline(database_oid: u32, next_due_at_ms: Option<i64>) {
    match next_due_at_ms.filter(|deadline| *deadline > 0) {
        Some(deadline_ms) => super::wake::schedule_maintenance_at_ms(database_oid, deadline_ms),
        None => super::wake::clear_maintenance_deadline(database_oid),
    }
}

struct MaintenanceRegistration {
    database_oid: u32,
}

impl Drop for MaintenanceRegistration {
    fn drop(&mut self) {
        super::wake::maintenance_stopped(self.database_oid);
    }
}

fn attach_signal_handlers() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP);
    unsafe {
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        pgrx::pg_sys::pqsignal(pgrx::pg_sys::SIGTERM as i32, Some(maintenance_sigterm));
        #[cfg(feature = "pg18")]
        pgrx::pg_sys::pqsignal_be(pgrx::pg_sys::SIGTERM as i32, Some(maintenance_sigterm));
    }
}

unsafe extern "C-unwind" fn maintenance_sigterm(signal: std::os::raw::c_int) {
    unsafe { pgrx::pg_sys::die(signal) }
}
