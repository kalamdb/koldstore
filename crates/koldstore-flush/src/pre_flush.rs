//! Pre-flush planning: in-memory counters → pending reservation descriptors.
//!
//! Syncs every non-zero [`ScopeCounterKey`] for a table into a single
//! `koldstore.pending` row per scope (create or update approximate `row_count`).
//! Flush initiation then selects only pending rows above the table hot-row
//! threshold so small scopes keep accumulating.

use koldstore_common::{FlushPolicy, ScopeCounterKey};

use crate::scope_counters::ScopeCounters;

/// One pending reservation to upsert into `koldstore.pending`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSegmentPlan {
    /// Counter key (table + optional scope).
    pub key: ScopeCounterKey,
    /// Approximate rows attributed to this pending reservation.
    pub row_count: u64,
}

/// Inputs for pre-flush counter → pending sync.
#[derive(Debug, Clone, Copy)]
pub struct PreFlushInput<'a> {
    pub table_oid: u32,
    pub policy: Option<&'a FlushPolicy>,
    /// When true, flush treats every non-zero pending as drainable.
    pub force: bool,
    /// Durable `(scope_key, count)` pairs used when the in-memory map is empty.
    pub durable_by_scope: &'a [(String, u64)],
}

/// Syncs in-memory (or reconciled) counters into pending-reservation plans.
///
/// Every non-zero key for the table becomes a plan. Threshold filtering happens
/// later at flush selection — pending rows below the hot-row limit keep
/// accumulating across pre-flush calls.
#[must_use]
pub fn plan_pending_segments(input: PreFlushInput<'_>) -> Vec<PendingSegmentPlan> {
    ScopeCounters::reconcile_table_if_empty(input.table_oid, input.durable_by_scope);

    ScopeCounters::keys_for_table(input.table_oid)
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .map(|(key, count)| PendingSegmentPlan {
            key,
            row_count: count,
        })
        .collect()
}

/// Resolves the per-table flush threshold from manage_table policy.
///
/// Prefers `hot_row_limit` (operator-facing retention), then
/// [`FlushPolicy::segment_row_threshold`].
#[must_use]
pub fn flush_pending_threshold(policy: Option<&FlushPolicy>) -> Option<u64> {
    policy.and_then(|policy| {
        policy
            .hot_row_limit
            .filter(|value| *value > 0)
            .or_else(|| policy.segment_row_threshold())
    })
}

/// Returns whether a pending approximate count should be flushed now.
#[must_use]
pub fn pending_is_flushable(row_count: u64, threshold: Option<u64>, force: bool) -> bool {
    if row_count == 0 {
        return false;
    }
    if force {
        return true;
    }
    match threshold {
        // No hot-row policy: drain every non-zero pending (manual / test path).
        None => true,
        // Operator policy: only flush scopes that exceeded the hot-row limit.
        Some(limit) => row_count > limit,
    }
}

/// Consumes reserved rows from in-memory counters after a successful scoped flush.
pub fn consume_pending_plans(plans: &[PendingSegmentPlan]) {
    for plan in plans {
        ScopeCounters::consume(&plan.key, plan.row_count);
    }
}
