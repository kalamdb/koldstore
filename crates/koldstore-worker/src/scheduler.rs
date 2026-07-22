//! Flush-check and idle-poll cadence helpers for the database worker.

use crate::policy::APPLY_IDLE_BACKOFF_MAX_MS;
use crate::TickResult;

/// Fairness budget for immediate retries after bounded apply work remains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingPollBudget {
    immediate_limit: u8,
    immediate_used: u8,
}

impl PendingPollBudget {
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

/// Next idle latch wait after an empty apply peek (exponential, capped).
#[must_use]
pub const fn next_idle_backoff_ms(current_ms: u64, floor_ms: u64) -> u64 {
    let floor = if floor_ms == 0 { 1 } else { floor_ms };
    if current_ms == 0 {
        return floor;
    }
    let doubled = current_ms.saturating_mul(2);
    if doubled < floor {
        floor
    } else if doubled > APPLY_IDLE_BACKOFF_MAX_MS {
        APPLY_IDLE_BACKOFF_MAX_MS
    } else {
        doubled
    }
}

/// Returns whether a flush eligibility check is due.
#[must_use]
pub const fn flush_check_due(
    last_check_secs: Option<i64>,
    now_secs: i64,
    interval_secs: i64,
) -> bool {
    let interval_secs = if interval_secs < 1 { 1 } else { interval_secs };
    match last_check_secs {
        None => true,
        Some(last) => now_secs.saturating_sub(last) >= interval_secs,
    }
}

#[cfg(test)]
mod tests {
    use super::{flush_check_due, next_idle_backoff_ms, PendingPollBudget};
    use crate::policy::APPLY_IDLE_BACKOFF_MAX_MS;
    use crate::TickResult;

    #[test]
    fn first_check_is_always_due() {
        assert!(flush_check_due(None, 100, 30));
    }

    #[test]
    fn check_waits_for_interval() {
        assert!(!flush_check_due(Some(100), 129, 30));
        assert!(flush_check_due(Some(100), 130, 30));
    }

    #[test]
    fn flush_check_due_clamps_interval_below_one_to_one() {
        assert!(!flush_check_due(Some(100), 100, 0));
        assert!(flush_check_due(Some(100), 101, 0));
    }

    #[test]
    fn pending_poll_budget_retries_four_ticks_then_yields() {
        let mut budget = PendingPollBudget::new(4);

        for _ in 0..4 {
            assert!(!budget.should_wait(TickResult::ContinuePending));
        }
        assert!(budget.should_wait(TickResult::ContinuePending));
        assert!(!budget.should_wait(TickResult::ContinuePending));
    }

    #[test]
    fn pending_poll_budget_resets_after_non_pending_work() {
        let mut budget = PendingPollBudget::new(2);

        assert!(!budget.should_wait(TickResult::ContinuePending));
        assert!(budget.should_wait(TickResult::Continue));
        assert!(!budget.should_wait(TickResult::ContinuePending));
        budget.reset();
        assert!(!budget.should_wait(TickResult::ContinuePending));
    }

    #[test]
    fn pending_poll_budget_waits_after_idle_tick() {
        let mut budget = PendingPollBudget::new(4);
        assert!(budget.should_wait(TickResult::ContinueIdle));
    }

    #[test]
    fn idle_backoff_doubles_until_cap() {
        assert_eq!(next_idle_backoff_ms(0, 100), 100);
        assert_eq!(next_idle_backoff_ms(100, 100), 200);
        assert_eq!(next_idle_backoff_ms(200, 100), 400);
        let mut ms = 100_u64;
        for _ in 0..20 {
            ms = next_idle_backoff_ms(ms, 100);
        }
        assert_eq!(ms, APPLY_IDLE_BACKOFF_MAX_MS);
    }
}
