//! Parquet footer summaries.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Min/max stats for one column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: serde_json::Value,
    pub max: serde_json::Value,
}

/// Row-group statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowGroupStats {
    pub row_group: usize,
    pub min_seq: Option<i64>,
    pub max_seq: Option<i64>,
    pub min_commit_seq: Option<i64>,
    pub max_commit_seq: Option<i64>,
}

/// File footer summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FooterSummary {
    pub row_groups: Vec<RowGroupStats>,
}

/// Segment-level metadata extracted from a written Parquet footer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentFooterMetadata {
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
    /// Final object byte size.
    pub byte_size: u64,
    /// Schema version written into the segment.
    pub schema_version: u32,
    /// Column stats used for manifest and local pruning metadata.
    pub column_stats: BTreeMap<String, ColumnStats>,
}

impl FooterSummary {
    /// Returns segment-level sequence and commit bounds from row groups.
    #[must_use]
    pub fn segment_bounds(&self) -> Option<(i64, i64, i64, i64)> {
        let min_seq = self.row_groups.iter().filter_map(|rg| rg.min_seq).min()?;
        let max_seq = self.row_groups.iter().filter_map(|rg| rg.max_seq).max()?;
        let min_commit_seq = self
            .row_groups
            .iter()
            .filter_map(|rg| rg.min_commit_seq)
            .min()?;
        let max_commit_seq = self
            .row_groups
            .iter()
            .filter_map(|rg| rg.max_commit_seq)
            .max()?;
        Some((min_seq, max_seq, min_commit_seq, max_commit_seq))
    }
}

impl SegmentFooterMetadata {
    /// Extracts segment metadata from footer row-group stats.
    #[must_use]
    pub fn from_footer(
        footer: &FooterSummary,
        row_count: u64,
        byte_size: u64,
        schema_version: u32,
        column_stats: Vec<(String, ColumnStats)>,
    ) -> Option<Self> {
        let (min_seq, max_seq, min_commit_seq, max_commit_seq) = footer.segment_bounds()?;

        Some(Self {
            min_seq,
            max_seq,
            min_commit_seq,
            max_commit_seq,
            row_count,
            byte_size,
            schema_version,
            column_stats: column_stats.into_iter().collect(),
        })
    }
}
