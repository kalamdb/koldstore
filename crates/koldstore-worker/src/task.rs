//! Task seam for work executed once per database-worker wake.
//!
//! Async mirror apply and built-in flush scheduling share the ensure/wait shell
//! in `pg_koldstore::worker` and report outcomes with [`TickResult`].

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
