//! Task seam for work executed once per database-worker poll tick.
//!
//! Async mirror apply implements this today. Flush job claiming is a planned
//! future implementor that can share the same ensure/poll shell.

/// Outcome of one worker tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickResult {
    /// Continue polling.
    Continue,
    /// Exit the worker loop (for example when infrastructure was removed).
    Stop,
}

/// One unit of work the database worker runs each poll tick.
///
/// Implementors must be memory-bounded per tick and idempotent under replay
/// (crash between durable write and slot advance must be safe).
pub trait DatabaseWorkerTask {
    /// Short name for logs and diagnostics.
    fn name(&self) -> &'static str;

    /// Runs one poll tick.
    ///
    /// # Errors
    ///
    /// Returns an error when the tick fails fatally; the adapter treats that as
    /// a worker ERROR exit. Transient empty work should return [`TickResult::Continue`].
    fn tick(&self) -> Result<TickResult, String>;
}
