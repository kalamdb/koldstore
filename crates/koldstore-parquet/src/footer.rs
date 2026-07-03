//! Parquet footer summaries.

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
