//! Transaction boundaries shared by KoldStore background workers.
//!
//! Queue-mode flush executors open and commit a transaction around each SPI
//! phase. The WAL applier and maintenance worker additionally need a protected
//! subtransaction so a PostgreSQL `ERROR` becomes a retryable Rust `Result`
//! instead of terminating the worker in the middle of shared-state cleanup.

use std::panic::AssertUnwindSafe;

use pgrx::pg_sys::panic::CaughtError;
use pgrx::PgTryBuilder;

/// Runs `body` inside one PostgreSQL transaction and commits on success.
///
/// # Errors
///
/// Returns `body`'s error after aborting the transaction. PostgreSQL ERROR
/// longjmps are converted to `Err` via [`pgrx::PgTryBuilder`].
pub(crate) fn run<R>(body: impl FnOnce() -> Result<R, String>) -> Result<R, String> {
    unsafe {
        pgrx::pg_sys::SetCurrentStatementStartTimestamp();
        pgrx::pg_sys::StartTransactionCommand();
        pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot());
    }
    let result = pgrx::PgTryBuilder::new(std::panic::AssertUnwindSafe(body))
        .catch_others(|error| Err(format_caught_error("worker transaction", error)))
        .catch_rust_panic(|error| Err(format_caught_error("worker transaction panic", error)))
        .execute();
    unsafe {
        if !pgrx::pg_sys::IsTransactionOrTransactionBlock() {
            return result;
        }
        if result.is_err() || pgrx::pg_sys::IsAbortedTransactionBlockState() {
            pgrx::pg_sys::AbortCurrentTransaction();
        } else {
            pgrx::pg_sys::PopActiveSnapshot();
            pgrx::pg_sys::CommitTransactionCommand();
        }
    }
    result
}

/// Runs `body` in a recoverable worker transaction.
///
/// A protected internal subtransaction absorbs PostgreSQL ERROR/longjmp and a
/// Rust panic so the caller can publish durable retry state and return cleanly.
/// This is used by both the always-on WAL applier and ephemeral maintenance.
///
/// # Errors
///
/// Returns the caught PostgreSQL/Rust error after rolling back the current
/// transaction, or an explicit error returned by `body`.
pub(crate) fn run_recoverable<R>(
    context: &str,
    body: impl FnOnce() -> Result<R, String>,
) -> Result<R, String> {
    unsafe {
        pgrx::pg_sys::SetCurrentStatementStartTimestamp();
        pgrx::pg_sys::StartTransactionCommand();
        pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot());
        pgrx::pg_sys::BeginInternalSubTransaction(std::ptr::null());
    }
    let result = PgTryBuilder::new(AssertUnwindSafe(body))
        .catch_others(|error| Err(format_caught_error(context, error)))
        .catch_rust_panic(|error| Err(format_caught_error(&format!("{context} panic"), error)))
        .execute();
    finish_subtransaction(result.is_ok());
    if unsafe { pgrx::pg_sys::IsAbortedTransactionBlockState() } {
        finish_outer_transaction(false);
        return Err(result
            .err()
            .unwrap_or_else(|| format!("{context} transaction aborted after postgres error")));
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
