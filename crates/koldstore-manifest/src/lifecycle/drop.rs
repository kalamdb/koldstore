//! Manifest drop and tombstone helpers.

use super::model::{Manifest, SegmentStatus};

impl Manifest {
    /// Marks all segments deleted for scope teardown planning.
    pub fn mark_all_segments_deleted(&mut self) {
        for segment in &mut self.segments {
            segment.status = SegmentStatus::Deleted;
        }
    }
}
