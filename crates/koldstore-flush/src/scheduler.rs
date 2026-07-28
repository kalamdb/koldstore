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
    if !options.auto_flush_enabled() || !options.flush_enabled() {
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

    fn row_limit_options(hot_row_limit: u64, min_flush_rows: u64) -> serde_json::Value {
        json!({
            "flush_policy": {
                "type": "row_limit",
                "hot_row_limit": hot_row_limit,
                "min_flush_rows": min_flush_rows,
                "max_rows_per_file": 1000,
                "max_rows_per_flush": 10_000
            }
        })
    }

    #[test]
    fn scheduler_skips_auto_flush_false() {
        let mut options = row_limit_options(10, 1);
        options
            .as_object_mut()
            .unwrap()
            .insert("auto_flush".into(), json!(false));
        assert!(!scheduler_should_flush(&options, 100));
    }

    #[test]
    fn scheduler_flushes_when_over_hot_limit() {
        let options = row_limit_options(10, 1);
        assert!(scheduler_should_flush(&options, 20));
        assert!(!scheduler_should_flush(&options, 10));
    }

    #[test]
    fn scheduler_skips_when_excess_below_min_flush_rows() {
        let options = row_limit_options(10, 100);
        assert!(!scheduler_should_flush(&options, 50));
    }

    #[test]
    fn scheduler_flushes_when_excess_meets_min_flush_rows() {
        let options = row_limit_options(10, 100);
        assert!(scheduler_should_flush(&options, 200));
    }

    #[test]
    fn scheduler_skips_missing_or_disabled_flush_policy() {
        assert!(!scheduler_should_flush(&json!({}), 1_000));
        assert!(!scheduler_should_flush(
            &json!({
                "flush_policy": {
                    "type": "row_limit",
                    "hot_row_limit": 0,
                    "min_flush_rows": 1,
                    "max_rows_per_file": 1000,
                    "max_rows_per_flush": 10_000
                }
            }),
            1_000
        ));
    }
}
