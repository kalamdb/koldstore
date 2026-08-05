//! Library naming and fairness policy for database-scoped workers.
//!
//! Appliers use `BGW_NEVER_RESTART` so intentional slot drop leaves them stopped.
//! A cluster launcher (auto-restarted) and the first backend query re-register
//! appliers after crashes or postmaster restart. Managed commits wake their
//! database worker through a coalescing shared generation.

/// Shared library name loaded by dynamic background workers.
pub const LIBRARY_NAME: &str = "koldstore";

/// Maximum budget-exhausted apply ticks retried before yielding to the latch.
///
/// This lets bounded catch-up avoid a full latch wait between every chunk while
/// ensuring foreground backends and scheduled flush work receive regular CPU.
pub const MAX_IMMEDIATE_PENDING_TICKS: u8 = 4;

/// Launcher poll interval while discovering databases that need an applier.
///
/// Kept in seconds-scale range: ensure is cheap when the oid set is unchanged,
/// and NEVER_RESTART appliers only need re-registration after crashes.
pub const LAUNCHER_POLL_INTERVAL_MS: u64 = 2_000;

/// Shared-memory wake registry slots (one entry per database OID seen).
///
/// Oversized for typical multi-DB clusters; a full registry fails closed to the
/// apply watchdog instead of blocking commit publishers.
pub const WAKE_REGISTRY_CAPACITY: usize = 256;

/// Initial soft-fail delay after a recoverable apply/flush SPI error.
pub const SOFT_FAIL_BACKOFF_MIN_MS: u64 = 100;

/// Cap on soft-fail exponential backoff so a sticky error still retries often.
pub const SOFT_FAIL_BACKOFF_MAX_MS: u64 = 30_000;

/// First empty-wake peek retry delay (async commit / WALWriter race).
pub const EMPTY_WAKE_RETRY_MIN_MS: u64 = 10;

/// Cap on empty-wake exponential peek retries.
pub const EMPTY_WAKE_RETRY_MAX_MS: u64 = 200;

/// Bounded window for empty-wake retries before acknowledging a false wake.
pub const EMPTY_WAKE_RETRY_WINDOW_MS: u64 = 1_000;

/// Returns the next soft-fail backoff after `current_ms` (0 = first failure).
#[must_use]
pub const fn next_soft_fail_backoff_ms(current_ms: u64) -> u64 {
    if current_ms == 0 {
        return SOFT_FAIL_BACKOFF_MIN_MS;
    }
    let doubled = current_ms.saturating_mul(2);
    if doubled < SOFT_FAIL_BACKOFF_MIN_MS {
        SOFT_FAIL_BACKOFF_MIN_MS
    } else if doubled > SOFT_FAIL_BACKOFF_MAX_MS {
        SOFT_FAIL_BACKOFF_MAX_MS
    } else {
        doubled
    }
}

#[cfg(test)]
mod tests {
    use super::{
        next_soft_fail_backoff_ms, SOFT_FAIL_BACKOFF_MAX_MS, SOFT_FAIL_BACKOFF_MIN_MS,
    };

    #[test]
    fn soft_fail_backoff_starts_at_min_and_doubles_to_cap() {
        assert_eq!(next_soft_fail_backoff_ms(0), SOFT_FAIL_BACKOFF_MIN_MS);
        assert_eq!(
            next_soft_fail_backoff_ms(SOFT_FAIL_BACKOFF_MIN_MS),
            SOFT_FAIL_BACKOFF_MIN_MS * 2
        );
        assert_eq!(
            next_soft_fail_backoff_ms(SOFT_FAIL_BACKOFF_MAX_MS),
            SOFT_FAIL_BACKOFF_MAX_MS
        );
        assert_eq!(
            next_soft_fail_backoff_ms(SOFT_FAIL_BACKOFF_MAX_MS / 2 + 1),
            SOFT_FAIL_BACKOFF_MAX_MS
        );
    }
}
