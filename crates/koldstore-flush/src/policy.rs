//! Flush policy and mirror-backed selection helpers.

use koldstore_common::SeqId;

pub use koldstore_common::{flush_enabled_from_options, hot_row_limit_from_options, FlushPolicy};

/// Mirror row available to flush policy evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorPolicyRow {
    /// JSON primary-key identity used by cleanup and diagnostics.
    pub pk_json: serde_json::Value,
    /// Latest-state mirror sequence.
    pub seq: SeqId,
}

/// Loads a flush policy from `koldstore.schemas.options`.
#[must_use]
pub fn flush_policy_from_options(options: &serde_json::Value) -> Option<FlushPolicy> {
    FlushPolicy::from_value(options)
}

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

/// Selects mirror rows eligible for the configured flush policy.
///
/// Row-limit policies keep at most `hot_row_limit` pending mirror rows by selecting
/// the oldest excess rows by `seq`.
#[must_use]
pub fn select_mirror_flush_candidates(
    policy: &FlushPolicy,
    rows: &[MirrorPolicyRow],
) -> Vec<MirrorPolicyRow> {
    let Some(limit) = policy.hot_row_limit else {
        return Vec::new();
    };

    let pending = rows.len() as u64;
    if pending <= limit {
        return Vec::new();
    }

    let excess = pending - limit;
    let min_flush_rows = policy.min_flush_rows.unwrap_or(1);
    let flush_count = flush_rows_for_excess(excess, min_flush_rows);
    if flush_count == 0 {
        return Vec::new();
    }

    let mut by_seq = rows.iter().collect::<Vec<_>>();
    by_seq.sort_by_key(|row| row.seq);
    by_seq
        .into_iter()
        .take(usize::try_from(flush_count).unwrap_or(usize::MAX))
        .cloned()
        .collect()
}
