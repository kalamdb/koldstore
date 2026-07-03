//! Background worker registration boundary.

/// Supported flush scheduling modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushWorkerMode {
    /// Built-in PostgreSQL background worker; requires shared preload.
    BuiltInBackgroundWorker,
    /// Operators can call SQL functions directly.
    SqlFunctionFallback,
    /// Optional pg_cron scheduling around SQL functions.
    PgCronFallback,
}

/// Returns whether built-in worker registration requires preload.
#[must_use]
pub const fn requires_shared_preload() -> bool {
    true
}

/// Returns supported flush scheduling modes in preference order.
#[must_use]
pub const fn flush_worker_modes() -> &'static [FlushWorkerMode] {
    &[
        FlushWorkerMode::BuiltInBackgroundWorker,
        FlushWorkerMode::SqlFunctionFallback,
        FlushWorkerMode::PgCronFallback,
    ]
}
