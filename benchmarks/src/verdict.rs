//! Benchmark threshold verdicts.

/// Hot UPDATE target overhead versus heap (p95 latency).
///
/// Clean-schema managed tables maintain a latest-state mirror through per-row
/// capture triggers, so local debug benchmark runs observe higher hot DML
/// overhead than heap. Short pgbench runs (1k ops / 3s) can spike p95 above
/// 2x while p50 stays near 2x; keep headroom for that variance without masking
/// large regressions.
pub const HOT_DML_MAX_OVERHEAD_RATIO: f64 = 2.6;

/// Hot INSERT target overhead versus heap.
///
/// Inserts pay mirror capture plus commit-time manifest counter flushes, so they
/// tolerate a higher ratio than updates while still guarding against regressions.
/// Short local runs commonly land near 4–5× p95 on debug builds; allow headroom
/// for scheduler noise without accepting multi-x regressions beyond that band.
pub const HOT_INSERT_MAX_OVERHEAD_RATIO: f64 = 5.0;

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
