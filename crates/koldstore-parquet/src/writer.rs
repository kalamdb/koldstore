//! Parquet writer surface.

use std::collections::BTreeMap;

use crate::footer::ColumnStats;

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
            row_count: 0,
            byte_size: 0,
            column_stats: BTreeMap::new(),
            pk_filter_kind: None,
            pk_filter_columns: Vec::new(),
        }
    }

    /// Builds a deterministic segment write plan with manifest metadata.
    #[must_use]
    pub fn plan_segment_with_metadata(
        &self,
        prefix: &str,
        batch: u32,
        metadata: SegmentMetadataInput,
    ) -> SegmentWritePlan {
        let mut plan = self.plan_segment(
            prefix,
            batch,
            metadata.min_seq,
            metadata.max_seq,
            metadata.min_commit_seq,
            metadata.max_commit_seq,
        );
        plan.row_count = metadata.row_count;
        plan.byte_size = metadata.byte_size;
        plan.column_stats = metadata.column_stats.into_iter().collect();
        plan.pk_filter_kind = (!metadata.pk_columns.is_empty()).then(|| "bloom".to_string());
        plan.pk_filter_columns = metadata.pk_columns;
        plan
    }
}

/// Segment metadata captured while writing a Parquet object.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentMetadataInput {
    /// Minimum `_seq`.
    pub min_seq: i64,
    /// Maximum `_seq`.
    pub max_seq: i64,
    /// Minimum `_commit_seq`.
    pub min_commit_seq: i64,
    /// Maximum `_commit_seq`.
    pub max_commit_seq: i64,
    /// Number of rows written.
    pub row_count: u64,
    /// Final object byte size.
    pub byte_size: u64,
    /// Primary-key columns eligible for bloom metadata.
    pub pk_columns: Vec<String>,
    /// Column stats used by segment pruning.
    pub column_stats: Vec<(String, ColumnStats)>,
}

/// Planned segment metadata produced by the writer.
#[derive(Debug, Clone, PartialEq)]
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
    /// Number of rows written.
    pub row_count: u64,
    /// Final object byte size.
    pub byte_size: u64,
    /// Column stats captured from the written footer.
    pub column_stats: BTreeMap<String, ColumnStats>,
    /// PK filter kind recorded for kalamdb-compatible manifests.
    pub pk_filter_kind: Option<String>,
    /// PK columns covered by the filter.
    pub pk_filter_columns: Vec<String>,
}
