//! Flush statistics types.
//!
//! Row/seq bounds for one flush attempt. Policy selection prefers O(1) manifest
//! counters plus an index-backed max-seq cutoff; force flush still uses mirror
//! aggregates (see `koldstore-mirror::read` and
//! `pg_koldstore::sql::flush::spi::resolve_flush_stats`).

/// Aggregated mirror sequence bounds selected for one flush attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushStats {
    /// Number of rows selected for flush.
    pub row_count: i64,
    /// Minimum selected `_seq`.
    pub min_seq: i64,
    /// Maximum selected `_seq`.
    pub max_seq: i64,
    /// Minimum selected `_commit_seq`.
    pub min_commit_seq: i64,
    /// Maximum selected `_commit_seq`.
    pub max_commit_seq: i64,
}

impl From<koldstore_mirror::MirrorSeqStats> for FlushStats {
    fn from(stats: koldstore_mirror::MirrorSeqStats) -> Self {
        Self {
            row_count: stats.row_count,
            min_seq: stats.min_seq,
            max_seq: stats.max_seq,
            min_commit_seq: stats.min_commit_seq,
            max_commit_seq: stats.max_commit_seq,
        }
    }
}

impl FlushStats {
    /// Returns zeroed stats for a no-op flush.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            row_count: 0,
            min_seq: 0,
            max_seq: 0,
            min_commit_seq: 0,
            max_commit_seq: 0,
        }
    }

    /// Derives flush stats from one encoded Parquet chunk.
    ///
    /// # Errors
    ///
    /// Returns an error when the row count does not fit in `i64`.
    pub fn from_cold_batch(batch: &koldstore_parquet::ColdRecordBatch) -> Result<Self, String> {
        Ok(Self {
            row_count: i64::try_from(batch.row_count).map_err(|error| error.to_string())?,
            min_seq: batch.min_seq,
            max_seq: batch.max_seq,
            min_commit_seq: batch.min_seq,
            max_commit_seq: batch.max_seq,
        })
    }
}

/// Resolved flush selection including optional mirror-op filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFlushSelection {
    /// Row/seq bounds for the flush attempt.
    pub stats: FlushStats,
    /// When set, mirror fetch is restricted to these operation codes.
    pub mirror_ops: Option<Vec<i16>>,
}

impl ResolvedFlushSelection {
    /// Builds a selection with no mirror-op filter.
    #[must_use]
    pub fn new(stats: FlushStats) -> Self {
        Self {
            stats,
            mirror_ops: None,
        }
    }
}

/// Validates that flush row selection matches resolved stats.
///
/// # Errors
///
/// Returns an error when the writer built a different number of rows than stats reported.
pub fn validate_flush_row_selection(
    stats_row_count: i64,
    write_row_count: usize,
) -> Result<(), String> {
    let write_row_count = i64::try_from(write_row_count).map_err(|error| error.to_string())?;
    if write_row_count != stats_row_count {
        return Err(format!(
            "flush row selection mismatch: stats reported {stats_row_count} rows but writer built {write_row_count} rows"
        ));
    }
    Ok(())
}
