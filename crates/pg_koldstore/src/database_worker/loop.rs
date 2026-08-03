//! Commit-wakeup latch loop and signal handling for the shared database worker.
//!
//! Managed commits advance a shared generation and set this worker's latch.
//! Each wake drains all generations observed before the apply pass, then on
//! `koldstore.flush_check_interval_seconds` evaluate auto-flush tables.
//!
//! A timeout remains as a correctness watchdog for missed notifications. The
//! auto-flush catalog probe is not run on every latch wake — only when a flush
//! check is due (or when deciding whether a slot-less worker should exit).
//!
//! Apply failures soft-fail with exponential backoff instead of FATAL so a
//! transient SPI error does not permanently stop catch-up.

use std::ffi::CString;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use koldstore_worker::{
    flush_check_due, EmptyWakeRetry, PendingDrainBudget, TickResult, WakeCursor, WakeGeneration,
    MAX_IMMEDIATE_PENDING_TICKS,
};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::PgTryBuilder;

use crate::async_mirror::task::AsyncMirrorTask;

use super::flush_task::{database_has_auto_flush_tables, run_flush_scheduler_tick};

const SOFT_FAIL_BACKOFF_MIN_MS: u64 = 100;
const SOFT_FAIL_BACKOFF_MAX_MS: u64 = 30_000;
const EMPTY_WAKE_RETRY_MIN_MS: u64 = 10;
const EMPTY_WAKE_RETRY_MAX_MS: u64 = 200;
const EMPTY_WAKE_RETRY_WINDOW_MS: u64 = 1_000;

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
    let registered_generation =
        super::wake::register_worker(database_oid).unwrap_or_else(|| WakeGeneration::new(0));
    let _wake_registration = WakeRegistration { database_oid };
    let mut wake_cursor = WakeCursor::new(registered_generation);

    let mut last_flush_check_secs: Option<i64> = None;
    // Cached so the latch path does not open an SPI transaction every wake.
    let mut auto_flush_cached = true;
    let mut apply_backoff_ms = 0_u64;
    let mut apply_retry_at = None::<Instant>;
    let mut startup_apply = true;
    let mut last_watchdog = Instant::now();
    let mut pending_drain_budget = PendingDrainBudget::new(MAX_IMMEDIATE_PENDING_TICKS);
    let worker_started = Instant::now();
    let mut empty_wake_retry = EmptyWakeRetry::new(
        Duration::from_millis(EMPTY_WAKE_RETRY_MIN_MS),
        Duration::from_millis(EMPTY_WAKE_RETRY_MAX_MS),
        Duration::from_millis(EMPTY_WAKE_RETRY_WINDOW_MS),
    );
    let mut empty_wake_retry_at = None::<Instant>;

    loop {
        let mut should_wait = true;
        let watchdog = Duration::from_millis(crate::guc::async_apply_watchdog_interval_ms());
        let slot_exists = crate::async_mirror::lifecycle::native_slot_exists_cstr(&slot_c);
        let now_secs = unix_now_secs();
        let interval = crate::guc::flush_check_interval_seconds();
        let flush_due = flush_check_due(last_flush_check_secs, now_secs, interval);

        if slot_exists {
            let generation = super::wake::generation(database_oid);
            let now = Instant::now();
            let watchdog_due = last_watchdog.elapsed() >= watchdog;
            let wake_pending = wake_cursor.is_pending(generation);
            let wake_retry_due = empty_wake_retry_at.is_none_or(|deadline| now >= deadline);
            let error_retry_due = apply_retry_at.is_some_and(|deadline| now >= deadline);
            let needs_apply = startup_apply
                || error_retry_due
                || (apply_retry_at.is_none() && (watchdog_due || (wake_pending && wake_retry_due)));
            if needs_apply {
                // One PostgreSQL transaction per apply tick: peek batches,
                // mirror SPI writes, and applied_lsn commit together. Soft-fail
                // logs and backs off instead of FATAL.
                // PostgreSQL emits `LOG` for every logical-decoding context
                // startup/consistent point. Reconnects, watchdogs, and commit
                // bursts would otherwise add routine noise. Scope the
                // threshold only around decoding; worker warnings and errors
                // remain visible after the guard restores the session value.
                //
                // Wake-driven empty peeks must not advance confirmed_flush:
                // otherwise unrelated WAL (and empty-wake retries) move the
                // slot before the watchdog. Watchdog/startup idle ticks still
                // advance so retained non-publication WAL can be skipped.
                let advance_slot_on_empty = startup_apply || watchdog_due || !wake_pending;
                let decoding_log_guard = DecodingLogGuard::suppress_routine_log_messages();
                let apply_result =
                    worker_transaction_result(|| async_task.tick_with(advance_slot_on_empty));
                drop(decoding_log_guard);
                match apply_result {
                    Ok(result @ TickResult::Continue) => {
                        apply_backoff_ms = 0;
                        apply_retry_at = None;
                        startup_apply = false;
                        // Observe the latest generation after the tick so commits
                        // that arrived while we held the apply lock are not lost.
                        wake_cursor.observe(super::wake::generation(database_oid));
                        empty_wake_retry.reset();
                        empty_wake_retry_at = None;
                        last_watchdog = Instant::now();
                        should_wait = pending_drain_budget.should_wait(result);
                    }
                    Ok(result @ TickResult::ContinueIdle) => {
                        apply_backoff_ms = 0;
                        apply_retry_at = None;
                        startup_apply = false;
                        if wake_pending && async_commit_wal_lag() {
                            // Insert LSN is still ahead of flush: the wake's
                            // commit may not be decodeable yet (sync_commit=off).
                            match empty_wake_retry.after_empty(worker_started.elapsed()) {
                                Some(delay) => {
                                    empty_wake_retry_at = Some(Instant::now() + delay);
                                }
                                None => {
                                    wake_cursor.observe(super::wake::generation(database_oid));
                                    empty_wake_retry.reset();
                                    empty_wake_retry_at = None;
                                }
                            }
                        } else {
                            wake_cursor.observe(super::wake::generation(database_oid));
                            empty_wake_retry.reset();
                            empty_wake_retry_at = None;
                        }
                        last_watchdog = Instant::now();
                        should_wait = pending_drain_budget.should_wait(result);
                    }
                    Ok(result @ TickResult::ContinuePending) => {
                        apply_backoff_ms = 0;
                        apply_retry_at = None;
                        startup_apply = false;
                        empty_wake_retry.reset();
                        empty_wake_retry_at = None;
                        should_wait = pending_drain_budget.should_wait(result);
                    }
                    Ok(result @ TickResult::Stop) => {
                        apply_backoff_ms = 0;
                        apply_retry_at = None;
                        startup_apply = false;
                        wake_cursor.observe(super::wake::generation(database_oid));
                        empty_wake_retry.reset();
                        empty_wake_retry_at = None;
                        last_watchdog = Instant::now();
                        should_wait = pending_drain_budget.should_wait(result);
                    }
                    Err(error) => {
                        pending_drain_budget.reset();
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
                        apply_retry_at =
                            Some(Instant::now() + Duration::from_millis(apply_backoff_ms));
                    }
                }
            }
        } else {
            pending_drain_budget.reset();
            startup_apply = true;
            apply_backoff_ms = 0;
            apply_retry_at = None;
            empty_wake_retry.reset();
            empty_wake_retry_at = None;
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
        let watchdog_wait_ms =
            u64::try_from(watchdog.saturating_sub(last_watchdog.elapsed()).as_millis())
                .unwrap_or(u64::MAX)
                .max(1);
        let flush_wait_ms = millis_until_flush_check(
            last_flush_check_secs,
            unix_now_secs(),
            crate::guc::flush_check_interval_seconds(),
        );
        let mut wait_ms = watchdog_wait_ms.min(flush_wait_ms);
        if let Some(deadline) = apply_retry_at {
            wait_ms = wait_ms.min(millis_until(deadline));
        }
        if let Some(deadline) = empty_wake_retry_at {
            wait_ms = wait_ms.min(millis_until(deadline));
        }
        let wait = Duration::from_millis(wait_ms);
        if should_wait && !BackgroundWorker::wait_latch(Some(wait)) {
            break;
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
    }
}

fn millis_until(deadline: Instant) -> u64 {
    u64::try_from(
        deadline
            .saturating_duration_since(Instant::now())
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
    .max(1)
}

/// Returns true when WAL has been inserted but not yet flushed far enough to
/// decode — the `synchronous_commit=off` gap empty-wake retries must bridge.
fn async_commit_wal_lag() -> bool {
    let insert = unsafe { pgrx::pg_sys::GetXLogInsertRecPtr() };
    let flush = unsafe { pgrx::pg_sys::GetFlushRecPtr(std::ptr::null_mut()) };
    insert > flush
}

struct WakeRegistration {
    database_oid: u32,
}

impl Drop for WakeRegistration {
    fn drop(&mut self) {
        super::wake::unregister_worker(self.database_oid);
    }
}

/// Temporarily hides PostgreSQL's routine logical-decoding `LOG` messages.
struct DecodingLogGuard {
    previous: std::os::raw::c_int,
}

impl DecodingLogGuard {
    fn suppress_routine_log_messages() -> Self {
        unsafe {
            let previous = pgrx::pg_sys::log_min_messages;
            // PostgreSQL ranks server-only `LOG` specially: WARNING and ERROR
            // thresholds still emit it. FATAL is the first level that hides
            // routine decoder LOG records. Caught failures are reported by the
            // worker after this guard restores the original threshold.
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

fn unix_now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn millis_until_flush_check(
    last_check_secs: Option<i64>,
    now_secs: i64,
    interval_secs: i64,
) -> u64 {
    let interval_secs = interval_secs.max(1);
    let Some(last_check_secs) = last_check_secs else {
        return 1;
    };
    let elapsed_secs = now_secs.saturating_sub(last_check_secs);
    if elapsed_secs >= interval_secs {
        return 1;
    }
    u64::try_from(interval_secs.saturating_sub(elapsed_secs))
        .unwrap_or(1)
        .saturating_mul(1_000)
        .max(1)
}
