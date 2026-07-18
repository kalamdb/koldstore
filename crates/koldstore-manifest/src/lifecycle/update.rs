//! Manifest update helpers.

use chrono::Utc;

use crate::model::{Manifest, ManifestBatchAppend, ManifestSegment, SegmentStatus};

impl Manifest {
    /// Appends a segment and updates watermarks for visible committed segments.
    pub fn append_segment(&mut self, segment: ManifestSegment) {
        let _ = self.append_segment_batch([segment]);
    }

    /// Appends several segments with one reserved vector growth and one manifest write.
    #[must_use]
    pub fn append_segment_batch(
        &mut self,
        segments: impl IntoIterator<Item = ManifestSegment>,
    ) -> ManifestBatchAppend {
        let segments = segments.into_iter();
        let (lower_bound, _) = segments.size_hint();
        self.segments.reserve(lower_bound);

        let mut appended_segments = 0usize;
        for segment in segments {
            if segment.status != SegmentStatus::Deleted {
                self.max_seq = self.max_seq.max(segment.max_seq);
                self.max_commit_seq = self.max_commit_seq.max(segment.max_commit_seq);
            }
            self.segments.push(segment);
            appended_segments += 1;
        }

        if appended_segments > 0 {
            self.updated_at = Utc::now();
        }

        ManifestBatchAppend {
            appended_segments,
            manifest_writes_required: usize::from(appended_segments > 0),
        }
    }

    /// Serializes the manifest to JSON.
    ///
    /// # Errors
    ///
    /// Returns JSON serialization errors.
    pub fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}
