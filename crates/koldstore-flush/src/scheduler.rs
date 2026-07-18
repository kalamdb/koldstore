//! Built-in auto-flush eligibility helpers (PostgreSQL-free).

use koldstore_common::{FlushPolicy, ManageTableOptions};
use serde_json::Value;

use crate::policy::policy_flush_row_count;

/// Returns whether the scheduler should enqueue/run a flush for these options.
#[must_use]
pub fn scheduler_should_flush(options: &Value, pending_rows: i64) -> bool {
    scheduler_should_flush_parsed(&ManageTableOptions::from_value(options), pending_rows)
}

/// Same as [`scheduler_should_flush`] after options are already decoded once.
#[must_use]
pub fn scheduler_should_flush_parsed(options: &ManageTableOptions, pending_rows: i64) -> bool {
    if !options.auto_flush_enabled() {
        return false;
    }
    let Some(policy) = options.flush_policy() else {
        return false;
    };
    policy_needs_flush(&policy, pending_rows)
}

/// Returns whether a decoded flush policy would move any rows for `pending_rows`.
#[must_use]
fn policy_needs_flush(policy: &FlushPolicy, pending_rows: i64) -> bool {
    policy_flush_row_count(pending_rows, policy) > 0
}

#[cfg(test)]
mod tests {
    use super::scheduler_should_flush;
    use serde_json::json;

    #[test]
    fn scheduler_skips_auto_flush_false() {
        let options = json!({
            "hot_row_limit": 10,
            "min_flush_rows": 1,
            "auto_flush": false
        });
        assert!(!scheduler_should_flush(&options, 100));
    }

    #[test]
    fn scheduler_flushes_when_over_hot_limit() {
        let options = json!({
            "hot_row_limit": 10,
            "min_flush_rows": 1
        });
        assert!(scheduler_should_flush(&options, 20));
        assert!(!scheduler_should_flush(&options, 10));
    }

    #[test]
    fn scheduler_skips_when_excess_below_min_flush_rows() {
        let options = json!({
            "hot_row_limit": 10,
            "min_flush_rows": 100
        });
        assert!(!scheduler_should_flush(&options, 50));
    }

    #[test]
    fn scheduler_flushes_when_excess_meets_min_flush_rows() {
        let options = json!({
            "hot_row_limit": 10,
            "min_flush_rows": 100
        });
        assert!(scheduler_should_flush(&options, 200));
    }

    #[test]
    fn scheduler_skips_missing_or_zero_hot_row_limit() {
        assert!(!scheduler_should_flush(&json!({}), 1_000));
        assert!(!scheduler_should_flush(
            &json!({ "hot_row_limit": 0, "min_flush_rows": 1 }),
            1_000
        ));
    }
}
