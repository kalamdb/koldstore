//! Cold segment visibility in `koldstore.cold_segments.status`.

use serde::{Deserialize, Serialize};

/// Segment visibility in `koldstore.cold_segments.status`.
///
/// Query planning includes only [`Self::Active`]. Flush inserts [`Self::Pending`]
/// until generation CAS activate. [`Self::Compacted`] / [`Self::Deleted`] are
/// reserved for future compaction/GC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentVisibility {
    Pending,
    Active,
    Compacted,
    Deleted,
}

impl SegmentVisibility {
    /// Returns whether this segment should be included in query planning.
    #[must_use]
    pub const fn is_query_visible(self) -> bool {
        matches!(self, Self::Active)
    }
}
