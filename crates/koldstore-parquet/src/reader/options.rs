//! Parquet read options, pruning filters, and per-segment read profiles.

use std::time::Duration;

use koldstore_common::SeqId;

/// Read options for projection and pruning.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetReadOptions {
    pub columns: Vec<String>,
    pub row_groups: Option<Vec<usize>>,
    pub seq_range: Option<SeqRange>,
    pub pk_values: Option<PkValues>,
    /// Stop after collecting this many decoded rows (`None` = read all selected).
    pub row_limit: Option<usize>,
    /// Outer wall-clock budget for one segment open/read (`None` = disabled).
    pub timeout: Option<Duration>,
    /// Diagnostic work requested by the caller.
    pub profile_mode: ParquetProfileMode,
}

impl ParquetReadOptions {
    /// Creates default read options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds projection columns.
    #[must_use]
    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Projects clean-schema change metadata columns.
    #[must_use]
    pub fn with_clean_change_metadata(mut self) -> Self {
        self.columns = vec![
            "seq".to_string(),
            "op".to_string(),
            "deleted".to_string(),
            "schema_version".to_string(),
        ];
        self
    }

    /// Adds selected row groups after footer/stat/bloom pruning.
    #[must_use]
    pub fn with_row_groups<I>(mut self, row_groups: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.row_groups = Some(row_groups.into_iter().collect());
        self
    }

    /// Adds clean-schema `seq` range pruning.
    #[must_use]
    pub fn with_clean_seq_range(mut self, min: SeqId, max: SeqId) -> Self {
        self.seq_range = Some(SeqRange {
            column: crate::schema::ColdMetadataColumn::Seq.name().to_string(),
            min,
            max,
        });
        self
    }

    /// Adds sequence range pruning for the given column name.
    #[must_use]
    pub fn with_seq_range(mut self, column: impl Into<String>, min: SeqId, max: SeqId) -> Self {
        self.seq_range = Some(SeqRange {
            column: column.into(),
            min,
            max,
        });
        self
    }

    /// Adds PK may-contain values for bloom/exact pruning.
    #[must_use]
    pub fn with_pk_values<I, S>(mut self, column: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.pk_values = Some(PkValues {
            column: column.into(),
            values: values.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Caps how many decoded rows are retained (early-stop the batch stream).
    #[must_use]
    pub fn with_row_limit(mut self, limit: usize) -> Self {
        self.row_limit = Some(limit);
        self
    }

    /// Sets the outer segment-read timeout (`None` / zero disables it).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout.filter(|value| !value.is_zero());
        self
    }

    /// Selects whether read counters and wall-clock timings are collected.
    #[must_use]
    pub fn with_profile_mode(mut self, mode: ParquetProfileMode) -> Self {
        self.profile_mode = mode;
        self
    }
}

/// Per-read diagnostic collection requested by a caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParquetProfileMode {
    /// Skip diagnostic counters, clocks, and profile-owned allocations.
    #[default]
    Disabled,
    /// Collect counters and pruning details without reading the wall clock.
    Counts,
    /// Collect counters, pruning details, and phase timings.
    CountsAndTiming,
}

impl ParquetProfileMode {
    pub(crate) const fn collects_counts(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    pub(crate) const fn collects_timing(self) -> bool {
        matches!(self, Self::CountsAndTiming)
    }
}

/// Sequence range pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqRange {
    pub column: String,
    pub min: SeqId,
    pub max: SeqId,
}

/// PK values for pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkValues {
    pub column: String,
    pub values: Vec<String>,
}

/// How PK bloom filters were used during a Parquet read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BloomPruneMode {
    /// No PK equality probe was requested.
    #[default]
    NotRequested,
    /// Min/max already left ≤1 row group; bloom pages were not fetched.
    SkippedAfterStats,
    /// Bloom pages were range-fetched to refine overlapping row groups.
    Applied,
}

pub use crate::page_prune::PageIndexPruneMode;

/// Per-segment ObjectStore Parquet read diagnostics for EXPLAIN / tracing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParquetReadProfile {
    /// Object key that was read.
    pub object_path: String,
    /// Known object size when provided by the catalog (bounded footer GET).
    pub file_size: Option<u64>,
    /// Footer was loaded via ObjectStore range/suffix GET before column data.
    pub footer_first: bool,
    /// Total row groups in the file footer.
    pub row_groups_total: usize,
    /// Row groups kept after min/max (+ optional bloom) pruning.
    pub row_groups_selected: Vec<usize>,
    /// Row groups skipped by pruning.
    pub row_groups_skipped: usize,
    /// Whether footer column-chunk min/max stats pruned any row groups.
    pub stats_pruned: bool,
    /// Bloom filter usage for this read.
    pub bloom: BloomPruneMode,
    /// Number of bloom filters actually range-fetched.
    pub bloom_filters_fetched: usize,
    /// Page-index usage for this read.
    pub page_index: PageIndexPruneMode,
    /// Data pages considered when page-index pruning ran.
    pub pages_total: usize,
    /// Pages kept after page min/max pruning.
    pub pages_selected: usize,
    /// Pages skipped by page min/max pruning.
    pub pages_skipped: usize,
    /// Projected application column names (plus required cold metadata).
    pub projected_columns: Vec<String>,
    /// PK equality probe values when present.
    pub pk_probe: Option<(String, Vec<String>)>,
    /// ObjectStore range GET call count (footer + bloom + column chunks).
    pub range_calls: u64,
    /// Total bytes returned by those range GETs.
    pub bytes_read: u64,
    /// Decoded clean cold rows after exact PK filter.
    pub rows_returned: usize,
    /// Footer metadata served from the backend-local cache (no footer GET).
    pub footer_cache_hit: bool,
    /// Wall time to construct the Parquet reader and load footer metadata.
    pub open_duration: Duration,
    /// Wall time after footer load through row-group scan and row decoding.
    pub scan_duration: Duration,
    /// Wall time awaited inside successful object-store range/suffix reads.
    ///
    /// This is a subset of `open_duration + scan_duration`, not an additive
    /// phase, because footer and column reads occur within those two phases.
    pub object_store_read_duration: Duration,
}

impl BloomPruneMode {
    /// Short label for EXPLAIN.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotRequested => "not_requested",
            Self::SkippedAfterStats => "skipped_after_stats",
            Self::Applied => "applied",
        }
    }
}

impl ParquetReadProfile {
    /// Compact I/O summary for EXPLAIN / tracing.
    #[must_use]
    pub fn format_io_summary(&self) -> String {
        let mut parts = Vec::new();
        if self.footer_first {
            parts.push("footer-first".to_string());
        }
        if self.footer_cache_hit {
            parts.push("footer_cache=hit".to_string());
        }
        parts.push(format!(
            "range_gets={}, bytes_read={}",
            self.range_calls, self.bytes_read
        ));
        if let Some(size) = self.file_size {
            if size > 0 && self.bytes_read < size {
                let pct = (self.bytes_read as f64 * 100.0) / size as f64;
                parts.push(format!("{pct:.1}% of object"));
            }
        }
        parts.join(", ")
    }

    /// Compact row-group prune summary for EXPLAIN / tracing.
    #[must_use]
    pub fn format_row_groups_summary(&self) -> String {
        let selected = if self.row_groups_selected.is_empty() {
            "none".to_string()
        } else {
            self.row_groups_selected
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        };
        format!(
            "total={}, selected=[{}], skipped={}, stats_pruned={}",
            self.row_groups_total, selected, self.row_groups_skipped, self.stats_pruned
        )
    }

    /// Compact bloom summary for EXPLAIN / tracing.
    #[must_use]
    pub fn format_bloom_summary(&self) -> String {
        match self.bloom {
            BloomPruneMode::NotRequested => "not_requested".to_string(),
            BloomPruneMode::SkippedAfterStats => {
                "skipped_after_stats (min/max left ≤1 row group)".to_string()
            }
            BloomPruneMode::Applied => {
                format!("applied, filters_fetched={}", self.bloom_filters_fetched)
            }
        }
    }

    /// Compact page-index prune summary for EXPLAIN / tracing.
    #[must_use]
    pub fn format_page_index_summary(&self) -> String {
        match self.page_index {
            PageIndexPruneMode::NotRequested => "not_requested".to_string(),
            PageIndexPruneMode::Absent => "absent".to_string(),
            PageIndexPruneMode::Applied => format!(
                "applied, pages_total={}, pages_selected={}, pages_skipped={}",
                self.pages_total, self.pages_selected, self.pages_skipped
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ParquetProfileMode;

    #[test]
    fn parquet_profile_modes_separate_regular_counts_and_timed_reads() {
        assert!(!ParquetProfileMode::Disabled.collects_counts());
        assert!(!ParquetProfileMode::Disabled.collects_timing());
        assert!(ParquetProfileMode::Counts.collects_counts());
        assert!(!ParquetProfileMode::Counts.collects_timing());
        assert!(ParquetProfileMode::CountsAndTiming.collects_counts());
        assert!(ParquetProfileMode::CountsAndTiming.collects_timing());
    }
}
