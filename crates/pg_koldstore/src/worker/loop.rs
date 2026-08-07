//! Ephemeral database-maintenance worker execution.
//!
//! A cluster supervisor starts at most one of these workers for a database when
//! a committed WAL generation, recovery request, or scheduling event exists.
//! The worker drains WAL through a fixed durable fence, reconciles local queue
//! work, waits for a very short burst grace, and exits when caught up.
//!
//! There is no permanent per-database polling/watchdog loop. Lost latch wakeups
//! cannot lose work because the logical slot and shared generations remain the
//! durable/recoverable source of truth.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::PgTryBuilder;

use crate::mirror::apply::{apply_bounded, capture_durable_wal_fence, BoundedApplyRequest};

const IDLE_GRACE: Duration = Duration::from_millis(200);

/// Runs one ephemeral database-maintenance worker.
pub(crate) fn run_async_mirror_applier(database_oid: u32) {
    attach_applier_signal_handlers();
    BackgroundWorker::connect_worker_to_spi_by_oid(
        Some(pgrx::pg_sys::Oid::from(database_oid)),
        None,
    );

    // A dynamic worker may run only when the supervisor reserved this database.
    // Stale/duplicate starts fail closed before logical decoding or catalog work.
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

    let mut first_pass = true;
    loop {
        let snapshot = super::wake::supervisor_snapshot(database_oid);
        let target_generation = snapshot.map(|state| state.wal_generation).unwrap_or(0);
        let maintenance_due = first_pass || snapshot.is_none_or(|state| state.maintenance_due());

        if maintenance_due {
            if let Err(error) = drain_wal_through_fixed_fence() {
                crate::observability::record_async_apply_error();
                pgrx::warning!(
                    "koldstore database maintenance db={database_oid} WAL apply deferred: {error}"
                );
                super::wake::request_recovery(database_oid);
                return;
            }
            super::wake::mark_wal_processed(database_oid, target_generation);
        }

        // Queue recovery + automatic policy evaluation are database-local and
        // happen only after WAL/counters are current. run_flush_scheduler_tick
        // may enqueue a durable job; its transaction callback publishes the
        // queue generation after commit, never before.
        match worker_transaction_result(super::flush_task::run_flush_scheduler_tick) {
            Ok(_) => super::wake::mark_maintenance_reconciled(database_oid),
            Err(error) => {
                pgrx::warning!(
                    "koldstore database maintenance db={database_oid} scheduler deferred: {error}"
                );
                super::wake::request_recovery(database_oid);
                return;
            }
        }

        first_pass = false;
        if super::wake::supervisor_snapshot(database_oid).is_some_and(|state| state.maintenance_due())
        {
            continue;
        }

        // A short burst grace avoids fork/exit churn during commit bursts. It is
        // an interruptible PostgreSQL latch wait, not a polling thread. New
        // commits wake the supervisor; the timeout guarantees this live worker
        // rechecks its generation even if it was not the direct wake target.
        if !BackgroundWorker::wait_latch(Some(IDLE_GRACE)) {
            return;
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
        if !super::wake::supervisor_snapshot(database_oid).is_some_and(|state| state.maintenance_due())
        {
            return;
        }
    }
}

/// Drains all WAL visible at one durable fence while honoring the normal
/// background row/time budgets. A generation sampled before the fence is not
/// acknowledged until this fixed boundary is completely processed.
fn drain_wal_through_fixed_fence() -> Result<(), String> {
    // XLogFlush makes synchronous_commit=off commits decodeable without forcing
    // foreground transactions to wait for KoldStore. This replaces the old
    // 10..200ms repeated empty-peek retry loop.
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

/// Temporarily hides PostgreSQL's routine logical-decoding LOG messages while
/// the decoder starts. Worker warnings/errors remain visible after the guard.
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

fn attach_applier_signal_handlers() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP);
    unsafe {
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        pgrx::pg_sys::pqsignal(pgrx::pg_sys::SIGTERM as i32, Some(applier_sigterm));
        #[cfg(feature = "pg18")]
        pgrx::pg_sys::pqsignal_be(pgrx::pg_sys::SIGTERM as i32, Some(applier_sigterm));
    }
}

unsafe extern "C-unwind" fn applier_sigterm(signal: std::os::raw::c_int) {
    unsafe { pgrx::pg_sys::die(signal) }
}

/// Runs `body` in a recoverable worker transaction.
///
/// PostgreSQL ERROR/longjmp and Rust panic are converted to a normal Result so
/// a failed attempt leaves the shared generation dirty and lets the supervisor
/// dispatch a fresh process rather than depending on one process staying alive.
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
