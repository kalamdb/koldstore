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

/// Invalid cold-file lifecycle transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentLifecycleError {
    /// Current status.
    pub from: &'static str,
    /// Requested status.
    pub to: &'static str,
}

impl std::fmt::Display for SegmentLifecycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid segment lifecycle transition: {} → {}",
            self.from, self.to
        )
    }
}

impl std::error::Error for SegmentLifecycleError {}

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

    /// Returns whether `from → to` is a valid cold-file lifecycle edge.
    ///
    /// Allowed edges (hard cutover):
    /// - `staged` → `published` | `orphaned`
    /// - `published` → `superseded` | `orphaned`
    /// - `superseded` → `deleting`
    /// - `deleting` → `deleted`
    /// - `orphaned` → `deleting` | `deleted`
    #[must_use]
    pub const fn can_transition(self, to: Self) -> bool {
        matches!(
            (self, to),
            (Self::Staged, Self::Published)
                | (Self::Staged, Self::Orphaned)
                | (Self::Published, Self::Superseded)
                | (Self::Published, Self::Orphaned)
                | (Self::Superseded, Self::Deleting)
                | (Self::Deleting, Self::Deleted)
                | (Self::Orphaned, Self::Deleting)
                | (Self::Orphaned, Self::Deleted)
        )
    }

    /// Validates and returns the next status.
    ///
    /// # Errors
    ///
    /// Returns an error when the transition is not allowed.
    pub fn transition(self, to: Self) -> Result<Self, SegmentLifecycleError> {
        if self.can_transition(to) {
            Ok(to)
        } else {
            Err(SegmentLifecycleError {
                from: self.as_str(),
                to: to.as_str(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_publish_and_compaction_paths_are_valid() {
        assert!(SegmentVisibility::Staged
            .transition(SegmentVisibility::Published)
            .is_ok());
        assert!(SegmentVisibility::Published
            .transition(SegmentVisibility::Superseded)
            .is_ok());
        assert!(SegmentVisibility::Superseded
            .transition(SegmentVisibility::Deleting)
            .is_ok());
        assert!(SegmentVisibility::Deleting
            .transition(SegmentVisibility::Deleted)
            .is_ok());
    }

    #[test]
    fn skips_and_regressions_are_rejected() {
        assert!(SegmentVisibility::Staged
            .transition(SegmentVisibility::Deleted)
            .is_err());
        assert!(SegmentVisibility::Published
            .transition(SegmentVisibility::Staged)
            .is_err());
        assert!(SegmentVisibility::Deleted
            .transition(SegmentVisibility::Published)
            .is_err());
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
