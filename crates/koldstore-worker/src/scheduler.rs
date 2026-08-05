//! Flush-check cadence and bounded-apply fairness for the database worker.

use crate::TickResult;

/// Fairness budget for immediate retries after bounded apply work remains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingDrainBudget {
    immediate_limit: u8,
    immediate_used: u8,
}

impl PendingDrainBudget {
    /// Creates a budget that allows `immediate_limit` retries before yielding.
    #[must_use]
    pub const fn new(immediate_limit: u8) -> Self {
        Self {
            immediate_limit,
            immediate_used: 0,
        }
    }

    /// Returns whether the worker should wait on its latch before the next tick.
    pub fn should_wait(&mut self, result: TickResult) -> bool {
        if result != TickResult::ContinuePending {
            self.reset();
            return true;
        }
        if self.immediate_used < self.immediate_limit {
            self.immediate_used = self.immediate_used.saturating_add(1);
            return false;
        }
        self.reset();
        true
    }

    /// Resets the immediate-retry count after a yield or error.
    pub const fn reset(&mut self) {
        self.immediate_used = 0;
    }
}

const fn flush_interval_secs(interval_secs: i64) -> i64 {
    if interval_secs < 1 {
        1
    } else {
        interval_secs
    }
}

/// Returns whether a flush eligibility check is due.
#[must_use]
pub const fn flush_check_due(
    last_check_secs: Option<i64>,
    now_secs: i64,
    interval_secs: i64,
) -> bool {
    let interval_secs = flush_interval_secs(interval_secs);
    match last_check_secs {
        None => true,
        Some(last) => now_secs.saturating_sub(last) >= interval_secs,
    }
}

/// Milliseconds until the next flush eligibility check (minimum 1).
///
/// Inverse of [`flush_check_due`]: when a check is due (including first run),
/// returns `1` so the latch wait wakes promptly.
#[must_use]
pub fn millis_until_flush_check(
    last_check_secs: Option<i64>,
    now_secs: i64,
    interval_secs: i64,
) -> u64 {
    let interval_secs = flush_interval_secs(interval_secs);
    let Some(last_check_secs) = last_check_secs else {
        return 1;
    };
    let elapsed_secs = now_secs.saturating_sub(last_check_secs);
    if elapsed_secs >= interval_secs {
        return 1;
    }
    u64::try_from(interval_secs.saturating_sub(elapsed_secs))
        .unwrap_or(1)
        .saturating_mul(1_000)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::{flush_check_due, millis_until_flush_check, PendingDrainBudget};
    use crate::TickResult;

    #[test]
    fn first_check_is_always_due() {
        assert!(flush_check_due(None, 100, 30));
        assert_eq!(millis_until_flush_check(None, 100, 30), 1);
    }

    #[test]
    fn check_waits_for_interval() {
        assert!(!flush_check_due(Some(100), 129, 30));
        assert_eq!(millis_until_flush_check(Some(100), 129, 30), 1_000);
        assert!(flush_check_due(Some(100), 130, 30));
        assert_eq!(millis_until_flush_check(Some(100), 130, 30), 1);
    }

    #[test]
    fn flush_check_due_clamps_interval_below_one_to_one() {
        assert!(!flush_check_due(Some(100), 100, 0));
        assert_eq!(millis_until_flush_check(Some(100), 100, 0), 1_000);
        assert!(flush_check_due(Some(100), 101, 0));
        assert_eq!(millis_until_flush_check(Some(100), 101, 0), 1);
    }

    #[test]
    fn flush_due_and_millis_until_agree() {
        for last in [None, Some(0_i64), Some(50), Some(100)] {
            for now in [0_i64, 50, 99, 100, 129, 130, 200] {
                for interval in [0_i64, 1, 30] {
                    let due = flush_check_due(last, now, interval);
                    let wait = millis_until_flush_check(last, now, interval);
                    assert_eq!(
                        due,
                        wait == 1,
                        "last={last:?} now={now} interval={interval}"
                    );
                }
            }
        }
    }

    #[test]
    fn pending_drain_budget_retries_four_ticks_then_yields() {
        let mut budget = PendingDrainBudget::new(4);

        for _ in 0..4 {
            assert!(!budget.should_wait(TickResult::ContinuePending));
        }
        assert!(budget.should_wait(TickResult::ContinuePending));
        assert!(!budget.should_wait(TickResult::ContinuePending));
    }

    #[test]
    fn pending_drain_budget_resets_after_non_pending_work() {
        let mut budget = PendingDrainBudget::new(2);

        assert!(!budget.should_wait(TickResult::ContinuePending));
        assert!(budget.should_wait(TickResult::Continue));
        assert!(!budget.should_wait(TickResult::ContinuePending));
        budget.reset();
        assert!(!budget.should_wait(TickResult::ContinuePending));
    }

    #[test]
    fn pending_drain_budget_waits_after_idle_tick() {
        let mut budget = PendingDrainBudget::new(4);
        assert!(budget.should_wait(TickResult::ContinueIdle));
    }
}
