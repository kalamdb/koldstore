//! Short BGWorker transactions for SPI catalog boundaries.
//!
//! Queue-mode flush executors open and commit a transaction around each SPI
//! phase (claim, mirror fetch, pending catalog insert, finalize, progress /
//! cancel, job terminal marks). Object-store upload runs **outside** these
//! transactions. Inline `flush_table` / `#[pg_test]` keep a single Nested
//! caller transaction and do not use this helper for mid-flush commits.
//!
//! Soft-fail subtransactions (async mirror apply) stay in [`super::r#loop`];
//! this module is the simple Start/Commit path shared by flush executors and
//! the async-mirror launcher.

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
        .catch_others(|error| {
            let message = match error {
                pgrx::pg_sys::panic::CaughtError::PostgresError(report)
                | pgrx::pg_sys::panic::CaughtError::ErrorReport(report) => {
                    report.message().to_string()
                }
                pgrx::pg_sys::panic::CaughtError::RustPanic { ereport, .. } => {
                    ereport.message().to_string()
                }
            };
            Err(format!("worker transaction: {message}"))
        })
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
