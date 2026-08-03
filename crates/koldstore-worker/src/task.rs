//! Task seam for work executed once per database-worker wake.
//!
//! Async mirror apply and built-in flush scheduling both implement this trait
//! and share the ensure/wait shell in `pg_koldstore::database_worker`.

/// Outcome of one worker tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickResult {
    /// Continue; applied work this tick (or non-idle progress).
    Continue,
    /// Peek found no publication changes; an empty commit wake may retry briefly.
    ContinueIdle,
    /// Tick budget exhausted with more WAL remaining — drain again without
    /// waiting for a new WAL insert position.
    ContinuePending,
    /// Exit the worker loop (for example when infrastructure was removed).
    Stop,
}

/// One unit of work the database worker runs after a wake.
///
/// Implementors must be memory-bounded per tick and idempotent under replay
/// (crash between durable write and slot advance must be safe).
pub trait DatabaseWorkerTask {
    /// Short name for logs and diagnostics.
    fn name(&self) -> &'static str;

    /// Runs one bounded work tick.
    ///
    /// # Errors
    ///
    /// Returns an error when the tick fails fatally; the adapter treats that as
    /// a worker ERROR exit. Transient empty work should return
    /// [`TickResult::ContinueIdle`].
    fn tick(&self) -> Result<TickResult, String>;
}
