//! Benchmark threshold verdicts.

/// Async foreground UPDATE p95 target versus a regular heap table.
pub const ASYNC_HOT_UPDATE_MAX_OVERHEAD_RATIO: f64 = 1.10;

/// Strict transactional UPDATE p95 target versus a regular heap table.
pub const STRICT_HOT_UPDATE_MAX_OVERHEAD_RATIO: f64 = 2.00;

/// Hot INSERT target overhead versus heap.
///
/// Inserts pay mirror capture plus commit-time manifest counter flushes, so they
/// tolerate a higher ratio than updates while still guarding against regressions.
/// Short local runs commonly land near 4–5× p95 on debug builds; allow headroom
/// for scheduler noise without accepting multi-x regressions beyond that band.
pub const HOT_INSERT_MAX_OVERHEAD_RATIO: f64 = 5.0;

/// PK lookup pruning target.
pub const PK_LOOKUP_MIN_ROW_GROUP_SKIP_RATIO: f64 = 0.90;

/// Verdict for async foreground UPDATE overhead versus a regular heap table.
#[must_use]
pub fn async_hot_update_within_threshold(heap_latency_ms: f64, koldstore_latency_ms: f64) -> bool {
    within_ratio(
        heap_latency_ms,
        koldstore_latency_ms,
        ASYNC_HOT_UPDATE_MAX_OVERHEAD_RATIO,
    )
}

/// Verdict for strict transactional UPDATE overhead versus a regular heap table.
#[must_use]
pub fn strict_hot_update_within_threshold(heap_latency_ms: f64, koldstore_latency_ms: f64) -> bool {
    within_ratio(
        heap_latency_ms,
        koldstore_latency_ms,
        STRICT_HOT_UPDATE_MAX_OVERHEAD_RATIO,
    )
}

fn within_ratio(heap_latency_ms: f64, koldstore_latency_ms: f64, max_ratio: f64) -> bool {
    heap_latency_ms > 0.0 && koldstore_latency_ms / heap_latency_ms <= max_ratio
}

/// Verdict for PK point lookup row-group pruning.
#[must_use]
pub fn pk_lookup_pruning_within_threshold(skipped_ratio: f64) -> bool {
    skipped_ratio >= PK_LOOKUP_MIN_ROW_GROUP_SKIP_RATIO
}

#[cfg(test)]
mod tests {
    use super::{async_hot_update_within_threshold, strict_hot_update_within_threshold};

    #[test]
    fn async_hot_update_gate_allows_ten_percent_overhead() {
        assert!(async_hot_update_within_threshold(1.0, 1.10));
        assert!(!async_hot_update_within_threshold(1.0, 1.11));
    }

    #[test]
    fn strict_hot_update_gate_allows_two_times_overhead() {
        assert!(strict_hot_update_within_threshold(1.0, 2.0));
        assert!(!strict_hot_update_within_threshold(1.0, 2.01));
    }
}
