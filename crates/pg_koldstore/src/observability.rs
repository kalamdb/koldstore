//! Tracing spans and counters.

use std::sync::atomic::{AtomicI64, Ordering};

/// Reserved for future PostgreSQL logging integration.
///
/// A `tracing-subscriber` is intentionally not installed in the extension
/// shared library: it adds significant binary size and PostgreSQL already
/// provides its own logging facilities.
pub fn init_tracing() {}

/// Tracing span names used by SQL and background paths.
pub const SPAN_NAMES: &[&str] = &[
    "koldstore.sql_api",
    "koldstore.dml_hook",
    "koldstore.flush",
    "koldstore.cold_reader_prune",
    "koldstore.merge_execute",
    "koldstore.object_store_io",
];

/// Known tracing span families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KoldstoreSpan<'a> {
    /// Public SQL API call.
    SqlApi { function: &'a str },
    /// Managed DML hook work.
    DmlHook { operation: &'a str },
    /// Flush phase.
    FlushPhase { phase: &'a str },
    /// Cold-reader pruning.
    ColdReaderPrune { table: &'a str },
    /// Merge-scan execution.
    MergeExecute { table: &'a str },
    /// Object-store I/O.
    ObjectStoreIo { operation: &'a str },
}

impl<'a> KoldstoreSpan<'a> {
    /// Returns the stable tracing span name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::SqlApi { .. } => "koldstore.sql_api",
            Self::DmlHook { .. } => "koldstore.dml_hook",
            Self::FlushPhase { .. } => "koldstore.flush",
            Self::ColdReaderPrune { .. } => "koldstore.cold_reader_prune",
            Self::MergeExecute { .. } => "koldstore.merge_execute",
            Self::ObjectStoreIo { .. } => "koldstore.object_store_io",
        }
    }

    /// Returns structured fields attached to the span.
    #[must_use]
    pub fn fields(&self) -> Vec<(&'static str, &'a str)> {
        match self {
            Self::SqlApi { function } => vec![("function", function)],
            Self::DmlHook { operation } => vec![("operation", operation)],
            Self::FlushPhase { phase } => vec![("phase", phase)],
            Self::ColdReaderPrune { table } | Self::MergeExecute { table } => {
                vec![("table", table)]
            }
            Self::ObjectStoreIo { operation } => vec![("operation", operation)],
        }
    }

    /// Creates an actual tracing span for runtime instrumentation.
    #[must_use]
    pub fn tracing_span(&self) -> tracing::Span {
        match self {
            Self::SqlApi { function } => {
                tracing::info_span!("koldstore.sql_api", function = *function)
            }
            Self::DmlHook { operation } => {
                tracing::info_span!("koldstore.dml_hook", operation = *operation)
            }
            Self::FlushPhase { phase } => tracing::info_span!("koldstore.flush", phase = *phase),
            Self::ColdReaderPrune { table } => {
                tracing::info_span!("koldstore.cold_reader_prune", table = *table)
            }
            Self::MergeExecute { table } => {
                tracing::info_span!("koldstore.merge_execute", table = *table)
            }
            Self::ObjectStoreIo { operation } => {
                tracing::info_span!("koldstore.object_store_io", operation = *operation)
            }
        }
    }
}

/// Testable object-store read counter.
#[derive(Debug, Default)]
pub struct ObjectStoreReadCounter {
    reads: AtomicI64,
}

impl ObjectStoreReadCounter {
    /// Records a hot DML operation. This intentionally does not increment reads.
    pub fn record_hot_dml_operation(&self) {}

    /// Records an object-store read.
    pub fn record_object_store_read(&self) {
        self.reads.fetch_add(1, Ordering::SeqCst);
    }

    /// Returns recorded object-store reads.
    #[must_use]
    pub fn reads(&self) -> i64 {
        self.reads.load(Ordering::SeqCst)
    }
}

/// Testable object-store I/O counters.
#[derive(Debug, Default)]
pub struct ObjectStoreIoCounter {
    reads: AtomicI64,
    writes: AtomicI64,
    deletes: AtomicI64,
}

impl ObjectStoreIoCounter {
    /// Records an object-store read operation.
    pub fn record_read(&self, operation: &str) {
        let _span = KoldstoreSpan::ObjectStoreIo { operation }.tracing_span();
        self.reads.fetch_add(1, Ordering::SeqCst);
    }

    /// Records an object-store write operation.
    pub fn record_write(&self, operation: &str) {
        let _span = KoldstoreSpan::ObjectStoreIo { operation }.tracing_span();
        self.writes.fetch_add(1, Ordering::SeqCst);
    }

    /// Records an object-store delete operation.
    pub fn record_delete(&self, operation: &str) {
        let _span = KoldstoreSpan::ObjectStoreIo { operation }.tracing_span();
        self.deletes.fetch_add(1, Ordering::SeqCst);
    }

    /// Returns recorded reads.
    #[must_use]
    pub fn reads(&self) -> i64 {
        self.reads.load(Ordering::SeqCst)
    }

    /// Returns recorded writes.
    #[must_use]
    pub fn writes(&self) -> i64 {
        self.writes.load(Ordering::SeqCst)
    }

    /// Returns recorded deletes.
    #[must_use]
    pub fn deletes(&self) -> i64 {
        self.deletes.load(Ordering::SeqCst)
    }
}

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
