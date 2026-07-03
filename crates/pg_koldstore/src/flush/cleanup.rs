//! Hot cleanup after manifest commit.

/// Planned hot cleanup behavior after a flush attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HotCleanupPlan {
    /// Whether live hot rows may be removed.
    pub remove_live_hot_rows: bool,
    /// Whether a hot tombstone must remain to mask older cold rows.
    pub retain_tombstone: bool,
}

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

/// Plans hot cleanup after manifest commit.
#[must_use]
pub const fn plan_hot_cleanup(
    manifest_committed: bool,
    cold_may_contain_pk: bool,
) -> HotCleanupPlan {
    HotCleanupPlan {
        remove_live_hot_rows: manifest_committed,
        retain_tombstone: retain_tombstone(cold_may_contain_pk),
    }
}
