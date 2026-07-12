//! Validated cold-file / manifest segment status transitions.
//!
//! Mirrors catalog [`koldstore_catalog::SegmentVisibility`] edges. Pending is
//! not a segment status — flush reservations live in `koldstore.pending`.

pub use koldstore_catalog::SegmentLifecycleError as SegmentStatusTransitionError;

use crate::model::SegmentStatus;

/// Durable compaction/GC hooks (status-only; object delete is job-owned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleHook {
    /// Mark replaced segments after a new published rewrite.
    Supersede,
    /// Retention elapsed; begin delete.
    BeginDelete,
    /// Object removed / delete acknowledged.
    AcknowledgeDeleted,
    /// Unreferenced without a valid owning lease.
    MarkOrphaned,
}

impl LifecycleHook {
    /// Applies a durable hook from the current status when the edge is valid.
    ///
    /// # Errors
    ///
    /// Returns an error when the hook is not allowed from `from`.
    pub fn apply(self, from: SegmentStatus) -> Result<SegmentStatus, SegmentStatusTransitionError> {
        let to = match self {
            Self::Supersede => SegmentStatus::Superseded,
            Self::BeginDelete => SegmentStatus::Deleting,
            Self::AcknowledgeDeleted => SegmentStatus::Deleted,
            Self::MarkOrphaned => SegmentStatus::Orphaned,
        };
        from.transition(to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_hook_chain() {
        let published = SegmentStatus::Published;
        let superseded = LifecycleHook::Supersede.apply(published).unwrap();
        let deleting = LifecycleHook::BeginDelete.apply(superseded).unwrap();
        let deleted = LifecycleHook::AcknowledgeDeleted.apply(deleting).unwrap();
        assert_eq!(deleted, SegmentStatus::Deleted);
    }

    #[test]
    fn orphan_from_staged() {
        assert_eq!(
            LifecycleHook::MarkOrphaned
                .apply(SegmentStatus::Staged)
                .unwrap(),
            SegmentStatus::Orphaned
        );
    }
}
