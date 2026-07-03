//! Benchmark threshold verdicts.

/// Hot DML target overhead versus heap.
pub const HOT_DML_MAX_OVERHEAD_RATIO: f64 = 1.10;

/// PK lookup pruning target.
pub const PK_LOOKUP_MIN_ROW_GROUP_SKIP_RATIO: f64 = 0.90;

/// Verdict for hot DML overhead compared with a regular heap table.
#[must_use]
pub fn hot_dml_within_threshold(heap_latency_ms: f64, koldstore_latency_ms: f64) -> bool {
    heap_latency_ms > 0.0 && koldstore_latency_ms / heap_latency_ms <= HOT_DML_MAX_OVERHEAD_RATIO
}

/// Verdict for PK point lookup row-group pruning.
#[must_use]
pub fn pk_lookup_pruning_within_threshold(skipped_ratio: f64) -> bool {
    skipped_ratio >= PK_LOOKUP_MIN_ROW_GROUP_SKIP_RATIO
}
