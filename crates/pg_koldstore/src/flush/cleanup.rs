//! Hot cleanup after manifest commit.

/// Returns whether cleanup may remove live hot rows.
#[must_use]
pub fn cleanup_allowed(manifest_committed: bool) -> bool {
    manifest_committed
}

/// Returns whether a tombstone should be retained after cleanup.
#[must_use]
pub const fn retain_tombstone(cold_may_contain_pk: bool) -> bool {
    cold_may_contain_pk
}
