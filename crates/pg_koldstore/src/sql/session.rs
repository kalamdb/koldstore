//! Session SQL helpers.

use std::sync::atomic::{AtomicI64, Ordering};

static NEXT_SEQ: AtomicI64 = AtomicI64::new(1);

/// Generates a monotonic Snowflake-like id for tests and SQL default use.
#[must_use]
#[cfg_attr(feature = "pg16", pgrx::pg_extern(name = "SNOWFLAKE_ID"))]
pub fn snowflake_id() -> i64 {
    NEXT_SEQ.fetch_add(1, Ordering::SeqCst)
}

/// Returns the active user scope when available.
#[must_use]
#[cfg_attr(feature = "pg16", pgrx::pg_extern(name = "koldstore_user_id"))]
pub fn koldstore_user_id() -> Option<String> {
    None
}

/// Normalizes an optional session user id.
#[must_use]
pub fn normalize_user_id(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
