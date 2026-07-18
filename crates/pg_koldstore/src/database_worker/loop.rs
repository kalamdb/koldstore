//! Latch poll loop and signal handling for the shared database worker.
//!
//! Each wake: apply async mirror when WAL advanced (if a slot exists), then on
//! `koldstore.flush_check_interval_seconds` evaluate auto-flush tables.
//!
//! Apply wakes use `koldstore.async_apply_poll_interval_ms` (default 100). The
//! auto-flush catalog probe is not run on every latch wake — only when a flush
//! check is due (or when deciding whether a slot-less worker should exit).
//!
//! Apply failures soft-fail with exponential backoff instead of FATAL so a
//! transient SPI error does not permanently stop catch-up.

use std::ffi::CString;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use koldstore_worker::{flush_check_due, DatabaseWorkerTask, TickResult};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::PgTryBuilder;

use crate::async_mirror::task::AsyncMirrorTask;

use super::flush_task::{database_has_auto_flush_tables, run_flush_scheduler_tick};

const SOFT_FAIL_BACKOFF_MIN_MS: u64 = 100;
const SOFT_FAIL_BACKOFF_MAX_MS: u64 = 30_000;

/// Runs the persistent database worker until neither async nor auto-flush work remains.
pub(crate) fn run_async_mirror_applier(database_oid: u32) {
    attach_applier_signal_handlers();
    BackgroundWorker::connect_worker_to_spi_by_oid(
        Some(pgrx::pg_sys::Oid::from(database_oid)),
        None,
    );

    let async_task = AsyncMirrorTask::new(database_oid);
    let slot = crate::async_mirror::lifecycle::slot_name(database_oid);
    let slot_c = CString::new(slot.as_str()).expect("deterministic slot name contains no NUL");

    let mut last_checked_wal = None;
    let mut last_flush_check_secs: Option<i64> = None;
    // Cached so the latch path does not open an SPI transaction every wake.
    let mut auto_flush_cached = true;
    let mut apply_backoff_ms = 0_u64;

    loop {
        let poll_ms = crate::guc::async_apply_poll_interval_ms().max(apply_backoff_ms);
        let poll = Duration::from_millis(poll_ms);
        let slot_exists = crate::async_mirror::lifecycle::native_slot_exists_cstr(&slot_c);
        let now_secs = unix_now_secs();
        let interval = crate::guc::flush_check_interval_seconds();
        let flush_due = flush_check_due(last_flush_check_secs, now_secs, interval);

        if slot_exists {
            let current_wal = current_wal_position();
            if last_checked_wal != Some(current_wal) || apply_backoff_ms > 0 {
                // One PostgreSQL transaction per apply tick: peek batches,
                // mirror SPI writes, and applied_lsn commit together. Soft-fail
                // logs and backs off instead of FATAL.
                match worker_transaction_result(|| async_task.tick()) {
                    Ok(TickResult::Continue) => {
                        apply_backoff_ms = 0;
                        last_checked_wal = Some(current_wal_position());
                    }
                    Ok(TickResult::ContinuePending) => {
                        apply_backoff_ms = 0;
                        last_checked_wal = None;
                    }
                    Ok(TickResult::Stop) => {
                        apply_backoff_ms = 0;
                    }
                    Err(error) => {
                        crate::observability::record_async_apply_error();
                        pgrx::log!(
                            "koldstore async mirror apply soft-failed (will retry): {error}"
                        );
                        apply_backoff_ms = if apply_backoff_ms == 0 {
                            SOFT_FAIL_BACKOFF_MIN_MS
                        } else {
                            apply_backoff_ms
                                .saturating_mul(2)
                                .clamp(SOFT_FAIL_BACKOFF_MIN_MS, SOFT_FAIL_BACKOFF_MAX_MS)
                        };
                        last_checked_wal = None;
                    }
                }
            }
        }

        if flush_due {
            // Single transaction: flush when due; skip EXISTS when a due table ran.
            // Soft-fail the whole flush tick on Postgres ERROR so a NEVER_RESTART
            // applier is not taken down by a transient SPI failure.
            match worker_transaction_result(|| {
                let has_auto = match run_flush_scheduler_tick() {
                    Ok(result) if result.had_due_table => true,
                    Ok(_) => match database_has_auto_flush_tables() {
                        Ok(value) => value,
                        Err(error) => {
                            pgrx::log!(
                                "koldstore database worker: auto_flush probe failed: {error}"
                            );
                            false
                        }
                    },
                    Err(error) => {
                        pgrx::log!("koldstore flush scheduler tick failed: {error}");
                        database_has_auto_flush_tables().unwrap_or_default()
                    }
                };
                Ok(has_auto)
            }) {
                Ok(value) => auto_flush_cached = value,
                Err(error) => {
                    pgrx::log!("koldstore database worker: flush tick soft-failed: {error}");
                    auto_flush_cached = true;
                }
            }
            last_flush_check_secs = Some(now_secs);
        }

        if !slot_exists && !auto_flush_cached {
            break;
        }
        if !BackgroundWorker::wait_latch(Some(poll)) {
            break;
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
    }
}

fn attach_applier_signal_handlers() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP);
    // Use PostgreSQL's standard SIGTERM handler while logical decoding is in
    // C code. It marks interrupts pending, allowing decoding and SPI safe
    // points to abort the transaction promptly during shutdown.
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
/// Soft-fail uses an internal subtransaction so a failpoint / SPI apply error
/// does not `AbortCurrentTransaction` the top-level worker txn (that path can
/// FATAL a `BGW_NEVER_RESTART` applier after logical-decoding portals).
///
/// Uncaught PostgreSQL `ERROR` longjmps are converted to `Err` via
/// [`PgTryBuilder`] so they also soft-fail instead of exiting the applier.
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
        .catch_others(|error| Err(format_caught_error("async worker", error)))
        .catch_rust_panic(|error| Err(format_caught_error("async worker panic", error)))
        .execute();
    finish_subtransaction(result.is_ok());
    if unsafe { pgrx::pg_sys::IsAbortedTransactionBlockState() } {
        finish_outer_transaction(false);
        return Err(result.err().unwrap_or_else(|| {
            "async worker transaction aborted after postgres error".to_string()
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

fn current_wal_position() -> u64 {
    unsafe { pgrx::pg_sys::GetXLogInsertRecPtr() }
}

fn unix_now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
