//! Flush policy and mirror-backed selection helpers.

pub use koldstore_common::FlushPolicy;

/// Computes how many excess mirror rows should move to cold storage.
///
/// When `excess` is below `min_flush_rows`, no rows are flushed. Otherwise the
/// flush count includes every full `min_flush_rows` chunk plus a final partial
/// chunk when the remainder is at least half of `min_flush_rows`.
#[must_use]
pub const fn flush_rows_for_excess(excess: u64, min_flush_rows: u64) -> u64 {
    if excess < min_flush_rows {
        return 0;
    }
    let full_chunks = excess / min_flush_rows;
    let remainder = excess % min_flush_rows;
    let partial = if remainder >= min_flush_rows / 2 {
        remainder
    } else {
        0
    };
    full_chunks * min_flush_rows + partial
}

/// Returns whether `selected` rows are enough to start a flush job.
///
/// Undersized selections (`0 < selected < max_rows_per_file`) must not enqueue:
/// they would write a tiny Parquet segment and still pay the full fence cost.
#[must_use]
pub const fn selected_rows_meet_file_minimum(selected: u64, max_rows_per_file: u64) -> bool {
    let minimum = if max_rows_per_file == 0 {
        1
    } else {
        max_rows_per_file
    };
    selected > 0 && selected >= minimum
}

/// Computes how many mirror rows a policy would flush for the current pending count.
///
/// This is a pure row-count computation: the actual oldest-by-`seq` cutoff is
/// resolved with an index-backed max-seq lookup, so no in-memory row list is
/// needed to answer "how many rows should flush".
///
/// Returns `0` when the selection would be smaller than `max_rows_per_file`, so
/// callers neither enqueue nor run a flush for an undersized segment.
#[must_use]
pub fn policy_flush_row_count(pending: i64, policy: &FlushPolicy) -> i64 {
    let limit = match policy {
        FlushPolicy::RowLimit { hot_row_limit, .. } => *hot_row_limit,
        FlushPolicy::OlderThan { .. } | FlushPolicy::Filter { .. } => return 0,
    };
    let pending = pending.max(0) as u64;
    if pending <= limit {
        return 0;
    }
    let excess = pending - limit;
    let selected =
        flush_rows_for_excess(excess, policy.min_flush_rows()).min(policy.max_rows_per_flush());
    if !selected_rows_meet_file_minimum(selected, policy.max_rows_per_file()) {
        return 0;
    }
    i64::try_from(selected).unwrap_or(0)
}
