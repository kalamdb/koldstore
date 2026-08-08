//! Pure apply-request contracts for bounded WAL mirror apply.
//!
//! PostgreSQL SPI peek/apply loops stay in `pg_koldstore`. This module owns the
//! request/outcome types and budget arithmetic so the extension adapter only
//! supplies GUC values and OID mapping.

use std::time::Duration;

use koldstore_common::{AppliedWalBoundary, WalFenceLsn};

/// Target-table mirror seq must be strictly greater than this floor after fence apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneSeqFloor(i64);

impl PruneSeqFloor {
    /// Wraps a mirror `max_seq` watermark.
    #[must_use]
    pub const fn new(max_seq: i64) -> Self {
        Self(max_seq)
    }

    /// Returns the raw floor value.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Request for a single bounded (or unbounded) async mirror apply pass.
#[derive(Debug, Clone)]
pub struct BoundedApplyRequest {
    /// When set, pass as `upto_lsn` to logical decoding.
    pub upper_bound: Option<WalFenceLsn>,
    /// Skip whole pgoutput transactions with `end_lsn <= skip_through`.
    pub skip_through: Option<AppliedWalBoundary>,
    /// When true, advance the slot to the previously committed durable checkpoint
    /// and record a new pending `applied_lsn`. Flush prune fences must use false.
    pub acknowledge_durable_checkpoint: bool,
    /// When true, an empty publication peek advances `confirmed_flush` through
    /// non-publication WAL. Wake-driven async-commit retries must use false so
    /// unrelated WAL cannot move the slot before the watchdog.
    pub advance_slot_on_empty: bool,
    /// When set, allocate sequences for this table (OID as `u32`) strictly above
    /// the floor.
    pub target_prune_floor: Option<(u32, PruneSeqFloor)>,
    /// Optional row budget override. `None` uses the background GUC; `Some(0)`
    /// means unlimited; `Some(n > 0)` caps source row changes in this pass.
    pub max_rows: Option<i64>,
    /// Optional wall-time budget override (milliseconds). Same semantics as
    /// [`Self::max_rows`].
    pub max_ms: Option<i64>,
}

impl BoundedApplyRequest {
    /// Default worker apply request (honors per-tick GUC budgets).
    #[must_use]
    pub fn available() -> Self {
        Self {
            upper_bound: None,
            skip_through: None,
            acknowledge_durable_checkpoint: true,
            advance_slot_on_empty: true,
            target_prune_floor: None,
            max_rows: None,
            max_ms: None,
        }
    }

    /// Strong-consistency fence: apply through a fixed durable WAL upper bound.
    #[must_use]
    pub fn upto_fence(fence: WalFenceLsn) -> Self {
        Self {
            upper_bound: Some(fence),
            skip_through: None,
            acknowledge_durable_checkpoint: true,
            advance_slot_on_empty: true,
            target_prune_floor: None,
            max_rows: Some(0),
            max_ms: Some(0),
        }
    }
}

/// Outcome of one bounded apply pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedApplyOutcome {
    /// Source row-change messages applied in this pass.
    pub row_changes: i64,
    /// Last applied durable boundary, if any.
    pub last_applied: Option<AppliedWalBoundary>,
    /// True when a configured row/time budget stopped the pass early.
    pub budget_exhausted: bool,
}

/// Resolves the effective row budget from an override or background default.
#[must_use]
pub fn resolve_row_budget(max_rows: Option<i64>, guc_default: i64) -> Option<i64> {
    match max_rows {
        Some(0) => None,
        Some(limit) if limit > 0 => Some(limit),
        Some(_) => None,
        None => (guc_default > 0).then_some(guc_default),
    }
}

/// Resolves the effective time budget from an override or background default.
#[must_use]
pub fn resolve_time_budget(max_ms: Option<i64>, guc_default_ms: i64) -> Option<Duration> {
    let ms = match max_ms {
        Some(0) => return None,
        Some(limit) if limit > 0 => limit,
        Some(_) => return None,
        None => {
            if guc_default_ms > 0 {
                guc_default_ms
            } else {
                return None;
            }
        }
    };
    Some(Duration::from_millis(u64::try_from(ms).unwrap_or(0)))
}

/// Returns true when either configured budget has been exhausted.
#[must_use]
pub fn budget_hit(
    row_budget: Option<i64>,
    time_budget: Option<Duration>,
    applied: i64,
    elapsed: Duration,
) -> bool {
    if let Some(limit) = row_budget {
        if applied >= limit {
            return true;
        }
    }
    if let Some(limit) = time_budget {
        if elapsed >= limit {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budgets_treat_zero_override_as_unlimited() {
        assert_eq!(resolve_row_budget(Some(0), 100), None);
        assert_eq!(resolve_time_budget(Some(0), 50), None);
        assert_eq!(resolve_row_budget(None, 100), Some(100));
        assert_eq!(
            resolve_time_budget(None, 50),
            Some(Duration::from_millis(50))
        );
    }

    #[test]
    fn budget_hit_checks_rows_then_time() {
        assert!(budget_hit(Some(10), None, 10, Duration::ZERO));
        assert!(!budget_hit(Some(10), None, 9, Duration::ZERO));
        assert!(budget_hit(
            None,
            Some(Duration::from_millis(5)),
            0,
            Duration::from_millis(5)
        ));
    }
}
