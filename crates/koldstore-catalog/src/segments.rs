//! Segment catalog rows and file lifecycle state.
//!
//! Owns the hard-cutover visibility vocabulary for cold files (`staged`…`orphaned`)
//! and the lightweight row shape used by catalog/flush code. Approximate flush
//! reservations live in `koldstore.pending`, not here.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Cold-file / catalog segment lifecycle (hard cutover).
///
/// Query-visible state is only [`Self::Published`]. Flush reservations are not
/// segment statuses — they live in `koldstore.pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentVisibility {
    /// Temp object written and validated; not yet published.
    Staged,
    /// Manifest commit succeeded; query-visible.
    Published,
    /// Replaced (e.g. compaction).
    Superseded,
    /// Retention passed; delete in progress.
    Deleting,
    /// Object removed or delete acknowledged.
    Deleted,
    /// Unreferenced / crash leftover.
    Orphaned,
}

impl SegmentVisibility {
    /// Returns whether this segment should be included in query planning.
    #[must_use]
    pub const fn is_query_visible(self) -> bool {
        matches!(self, Self::Published)
    }

    /// Catalog SQL status literal.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Staged => "staged",
            Self::Published => "published",
            Self::Superseded => "superseded",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
            Self::Orphaned => "orphaned",
        }
    }
}

/// Catalog segment row (`koldstore.segments`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Segment {
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
