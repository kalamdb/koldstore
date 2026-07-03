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
