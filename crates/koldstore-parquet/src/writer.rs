//! Parquet writer surface.

/// Writer options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterOptions {
    pub compression: String,
    pub row_group_size: usize,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            compression: "snappy".to_string(),
            row_group_size: 64 * 1024,
        }
    }
}

/// Segment writer placeholder.
#[derive(Debug, Clone)]
pub struct ParquetSegmentWriter {
    pub options: WriterOptions,
}

impl ParquetSegmentWriter {
    /// Creates a segment writer.
    #[must_use]
    pub fn new(options: WriterOptions) -> Self {
        Self { options }
    }

    /// Builds a deterministic segment write plan.
    #[must_use]
    pub fn plan_segment(
        &self,
        prefix: &str,
        batch: u32,
        min_seq: i64,
        max_seq: i64,
        min_commit_seq: i64,
        max_commit_seq: i64,
    ) -> SegmentWritePlan {
        let prefix = prefix.trim_matches('/');
        SegmentWritePlan {
            object_path: format!("{prefix}/batch-{batch}.parquet"),
            min_seq,
            max_seq,
            min_commit_seq,
            max_commit_seq,
            compression: self.options.compression.clone(),
        }
    }
}

/// Planned segment metadata produced by the writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentWritePlan {
    /// Final object path.
    pub object_path: String,
    /// Minimum `_seq`.
    pub min_seq: i64,
    /// Maximum `_seq`.
    pub max_seq: i64,
    /// Minimum `_commit_seq`.
    pub min_commit_seq: i64,
    /// Maximum `_commit_seq`.
    pub max_commit_seq: i64,
    /// Compression codec.
    pub compression: String,
}
