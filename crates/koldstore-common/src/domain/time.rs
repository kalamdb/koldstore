//! Wall-clock helpers shared by deadline scheduling (not monotonic clocks).

use std::time::{SystemTime, UNIX_EPOCH};

/// Current Unix epoch time in milliseconds.
///
/// Used for flush/maintenance wake deadlines. Returns `0` if the system clock
/// is before the Unix epoch (should not happen on supported hosts).
#[must_use]
pub fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
