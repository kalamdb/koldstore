//! Ephemeral database-maintenance background worker.
//!
//! The cluster supervisor owns registration. At most one maintenance worker is
//! active per database. A worker drains committed WAL through a fixed durable
//! fence, performs database-local recovery/auto-flush scheduling, waits briefly
//! to coalesce a write burst, and exits when the database is caught up.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use koldstore_worker::{async_mirror_worker_type, DatabaseOid, LIBRARY_NAME};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::PgTryBuilder;

use crate::mirror::apply::{apply_bounded, capture_durable_wal_fence, BoundedApplyRequest};

const MAINTENANCE_FUNCTION: &str = "koldstore_maintenance_worker_main";
const IDLE_GRACE: Duration = Duration::from_millis(200);

/// Registers one already-reserved maintenance worker without waiting for startup.
///
/// Only the static cluster supervisor may call this in production. Shared state
/// reserves the database before registration so a second worker cannot race in
/// while PostgreSQL is still starting the first process.
pub(crate) fn register_maintenance_from_supervisor(database_oid: u32) -> Result<(), String> {
    let database_oid = DatabaseOid::new(database_oid);
    let worker_type = async_mirror_worker_type(database_oid);
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
        let target_wal_generation = snapshot.wal_generation;
        let target_maintenance_generation = snapshot.maintenance_generation;
        let recovery_requested =
            snapshot.event_flags & koldstore_worker::EVENT_RECOVERY_REQUIRED != 0;
        let wal_due =
            recovery_requested || snapshot.wal_generation != snapshot.wal_processed_generation;

        if wal_due {
            if let Err(error) = drain_wal_through_fixed_fence() {
                crate::observability::record_async_apply_error();
                pgrx::warning!(
                    "koldstore maintenance worker db={database_oid} WAL apply deferred: {error}"
                );
                super::wake::request_recovery(database_oid);
                return;
            }
            super::wake::mark_wal_processed(database_oid, target_wal_generation);
        }

        // A SIGKILL/FATAL executor cannot run Rust Drop. The supervisor marks
        // the database RECOVERY_REQUIRED after native child-exit notification;
        // this transaction reclaims the durable job before redispatch.
        let maintenance_result = worker_transaction_result(|| {
            if recovery_requested {
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
            Ok(_) => {
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

        if super::wake::supervisor_snapshot(database_oid)
            .is_some_and(|state| state.maintenance_due())
        {
            continue;
        }

        // Brief interruptible grace amortizes fork/exit cost across commit bursts.
        // New commits set this worker's latch directly while it is alive.
        if !BackgroundWorker::wait_latch(Some(IDLE_GRACE)) {
            return;
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
        if !super::wake::supervisor_snapshot(database_oid)
            .is_some_and(|state| state.maintenance_due())
        {
            return;
        }
    }
}

/// Drains all WAL visible at one fixed durable fence while respecting bounded
/// apply budgets. `synchronous_commit=off` stays foreground-asynchronous: this
/// worker performs XLogFlush, not the application backend.
fn drain_wal_through_fixed_fence() -> Result<(), String> {
    let fence = capture_durable_wal_fence()?;
    loop {
        let decoding_log_guard = DecodingLogGuard::suppress_routine_log_messages();
        let outcome = worker_transaction_result(|| {
            let mut request = BoundedApplyRequest::available();
            request.upper_bound = Some(fence);
            request.advance_slot_on_empty = true;
            apply_bounded(request)
        });
        drop(decoding_log_guard);
        let outcome = outcome?;
        crate::observability::record_async_apply_tick(outcome.row_changes, 0);
        if !outcome.budget_exhausted {
            return Ok(());
        }
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

struct DecodingLogGuard {
    previous: std::os::raw::c_int,
}

impl DecodingLogGuard {
    fn suppress_routine_log_messages() -> Self {
        unsafe {
            let previous = pgrx::pg_sys::log_min_messages;
            pgrx::pg_sys::log_min_messages = pgrx::pg_sys::FATAL as std::os::raw::c_int;
            Self { previous }
        }
    }
}

impl Drop for DecodingLogGuard {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::log_min_messages = self.previous;
        }
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

/// Runs `body` in a recoverable worker transaction. PostgreSQL ERROR/longjmp and
/// Rust panic become a normal Result so durable generations remain retryable.
pub(crate) fn worker_transaction_result<R>(
    body: impl FnOnce() -> Result<R, String>,
) -> Result<R, String> {
    unsafe {
        pgrx::pg_sys::SetCurrentStatementStartTimestamp();
        pgrx::pg_sys::StartTransactionCommand();
        pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot());
        pgrx::pg_sys::BeginInternalSubTransaction(std::ptr::null());
    }
    let result = PgTryBuilder::new(AssertUnwindSafe(body))
        .catch_others(|error| Err(format_caught_error("maintenance worker", error)))
        .catch_rust_panic(|error| Err(format_caught_error("maintenance worker panic", error)))
        .execute();
    finish_subtransaction(result.is_ok());
    if unsafe { pgrx::pg_sys::IsAbortedTransactionBlockState() } {
        finish_outer_transaction(false);
        return Err(result.err().unwrap_or_else(|| {
            "maintenance worker transaction aborted after postgres error".to_string()
        }));
    }
    finish_outer_transaction(true);
    result
}

fn finish_subtransaction(release: bool) {
    unsafe {
        if pgrx::pg_sys::GetCurrentTransactionNestLevel() <= 1 {
            return;
        }
        if release && !pgrx::pg_sys::IsAbortedTransactionBlockState() {
            pgrx::pg_sys::ReleaseCurrentSubTransaction();
        } else {
            pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
        }
    }
}

fn finish_outer_transaction(commit: bool) {
    unsafe {
        if !pgrx::pg_sys::IsTransactionOrTransactionBlock() {
            return;
        }
        if !commit || pgrx::pg_sys::IsAbortedTransactionBlockState() {
            pgrx::pg_sys::AbortCurrentTransaction();
            return;
        }
        pgrx::pg_sys::PopActiveSnapshot();
        pgrx::pg_sys::CommitTransactionCommand();
    }
}

fn format_caught_error(context: &str, error: CaughtError) -> String {
    match error {
        CaughtError::PostgresError(report) | CaughtError::ErrorReport(report) => {
            format!("{context}: {}", report.message())
        }
        CaughtError::RustPanic { ereport, payload } => {
            let detail = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("rust panic");
            format!("{context}: {} ({detail})", ereport.message())
        }
    }
}
