//! Tracing spans and counters.

use std::sync::atomic::{AtomicI64, Ordering};

/// Initializes tracing for non-PostgreSQL tests.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
}

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
