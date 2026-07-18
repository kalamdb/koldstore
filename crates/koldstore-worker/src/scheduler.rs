//! Flush-check cadence helpers for the database worker.

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
    use super::flush_check_due;

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
}
