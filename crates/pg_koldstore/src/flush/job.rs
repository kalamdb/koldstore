//! Flush job state transitions.

/// Manifest sync states used by flush jobs.
pub const FLUSH_STATES: &[&str] = &["pending_write", "syncing", "in_sync", "stale", "error"];

/// Flush metadata written after manifest commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdMetadataUpdate {
    /// Minimum `_seq`.
    pub min_seq: i64,
    /// Maximum `_seq`.
    pub max_seq: i64,
    /// Minimum `_commit_seq`.
    pub min_commit_seq: i64,
    /// Maximum `_commit_seq`.
    pub max_commit_seq: i64,
    /// Segment row count.
    pub row_count: u64,
    /// Segment byte size.
    pub byte_size: u64,
}

/// Returns the next manifest sync state after a successful flush.
#[must_use]
pub const fn successful_flush_state(remaining_hot_rows: bool) -> &'static str {
    if remaining_hot_rows {
        "pending_write"
    } else {
        "in_sync"
    }
}

/// Returns whether a bounded flush batch should continue.
#[must_use]
pub const fn should_continue_batch(scanned_rows: usize, batch_size: usize) -> bool {
    scanned_rows >= batch_size
}
