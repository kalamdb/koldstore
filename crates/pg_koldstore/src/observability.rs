//! Process-local async apply metrics.
//!
//! A `tracing-subscriber` is intentionally not installed in the extension
//! shared library: it adds significant binary size and PostgreSQL already
//! provides its own logging facilities.

use std::sync::atomic::{AtomicI64, Ordering};

/// Reserved for future PostgreSQL logging integration.
pub fn init_tracing() {}

/// Process-local async apply counters (reset on backend restart).
static APPLY_ROWS_TOTAL: AtomicI64 = AtomicI64::new(0);
static APPLY_TICKS_TOTAL: AtomicI64 = AtomicI64::new(0);
static LAST_APPLY_ROWS: AtomicI64 = AtomicI64::new(0);
static LAST_APPLY_ELAPSED_MS: AtomicI64 = AtomicI64::new(0);
static APPLY_ERROR_TOTAL: AtomicI64 = AtomicI64::new(0);
static APPLY_HEALTH_OK: AtomicI64 = AtomicI64::new(1);

/// Records one completed apply tick for rate/lag observability.
pub fn record_async_apply_tick(row_changes: i64, elapsed_ms: i64) {
    APPLY_ROWS_TOTAL.fetch_add(row_changes.max(0), Ordering::Relaxed);
    APPLY_TICKS_TOTAL.fetch_add(1, Ordering::Relaxed);
    LAST_APPLY_ROWS.store(row_changes, Ordering::Relaxed);
    LAST_APPLY_ELAPSED_MS.store(elapsed_ms.max(0), Ordering::Relaxed);
    APPLY_HEALTH_OK.store(1, Ordering::Relaxed);
}

/// Records a soft-failed apply tick (backoff path).
pub fn record_async_apply_error() {
    APPLY_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
    APPLY_HEALTH_OK.store(0, Ordering::Relaxed);
}

/// Snapshot of process-local async apply counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncApplyMetrics {
    pub rows_total: i64,
    pub ticks_total: i64,
    pub last_rows: i64,
    pub last_elapsed_ms: i64,
    pub error_total: i64,
    pub healthy: bool,
}

/// Returns process-local async apply counters.
#[must_use]
pub fn async_apply_metrics() -> AsyncApplyMetrics {
    AsyncApplyMetrics {
        rows_total: APPLY_ROWS_TOTAL.load(Ordering::Relaxed),
        ticks_total: APPLY_TICKS_TOTAL.load(Ordering::Relaxed),
        last_rows: LAST_APPLY_ROWS.load(Ordering::Relaxed),
        last_elapsed_ms: LAST_APPLY_ELAPSED_MS.load(Ordering::Relaxed),
        error_total: APPLY_ERROR_TOTAL.load(Ordering::Relaxed),
        healthy: APPLY_HEALTH_OK.load(Ordering::Relaxed) != 0,
    }
}
