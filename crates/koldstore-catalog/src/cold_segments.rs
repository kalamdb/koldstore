//! Cold segment catalog rows.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Segment visibility.
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

/// Cold segment catalog row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColdSegment {
    pub segment_id: Uuid,
    pub table_oid: u32,
    pub scope_key: Option<String>,
    pub object_path: String,
    pub min_seq: i64,
    pub max_seq: i64,
    pub min_commit_seq: i64,
    pub max_commit_seq: i64,
    pub status: SegmentVisibility,
}
