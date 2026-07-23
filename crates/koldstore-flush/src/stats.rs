//! Flush statistics types and pure selection helpers.
//!
//! Row/seq bounds for one flush attempt. Policy selection prefers O(1) manifest
//! counters plus an index-backed max-seq cutoff; force flush still uses mirror
//! aggregates. SPI adapters in `pg_koldstore` supply the inputs.

use koldstore_common::{FlushPolicy, MirrorOperation};

use crate::policy::policy_flush_row_count;

/// Cap for force-flush tombstone-only selection.
pub const FORCE_TOMBSTONE_ONLY_CAP: i64 = 4_096;

/// Cap for one force-flush wave when draining a large mirror backlog.
///
/// Matches [`koldstore_common::DEFAULT_MAX_ROWS_PER_FLUSH`] so force and policy
/// waves share the same memory ceiling; `flush_prepared_table` keeps draining
/// through the start-of-job seq watermark (or the catch-up wave budget).
pub const FORCE_FLUSH_WAVE_ROW_CAP: i64 = koldstore_common::DEFAULT_MAX_ROWS_PER_FLUSH as i64;

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

    /// Derives flush stats from one fully encoded segment chunk.
    ///
    /// # Errors
    ///
    /// Returns an error when the row count does not fit in `i64`.
    pub fn from_write_chunk(chunk: &crate::write::FlushWriteChunk) -> Result<Self, String> {
        Ok(Self {
            row_count: i64::try_from(chunk.row_count).map_err(|error| error.to_string())?,
            min_seq: chunk.min_seq,
            max_seq: chunk.max_seq,
            min_commit_seq: chunk.min_seq,
            max_commit_seq: chunk.max_seq,
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

/// Resolves a normal (non-force) flush selection from policy math and cutoff.
#[must_use]
pub fn resolve_policy_flush_selection(
    pending_rows: i64,
    policy: Option<&FlushPolicy>,
    cutoff: Option<(i64, i64)>,
    full_mirror: FlushStats,
) -> ResolvedFlushSelection {
    if pending_rows == 0 {
        return ResolvedFlushSelection::new(FlushStats::empty());
    }
    let Some(policy) = policy else {
        return ResolvedFlushSelection::new(full_mirror);
    };
    let flush_count = policy_flush_row_count(pending_rows, policy);
    if flush_count == 0 {
        return ResolvedFlushSelection::new(FlushStats::empty());
    }
    let Some((selected_count, max_seq)) = cutoff else {
        return ResolvedFlushSelection::new(FlushStats::empty());
    };
    if selected_count == 0 || max_seq == 0 {
        return ResolvedFlushSelection::new(FlushStats::empty());
    }
    ResolvedFlushSelection::new(FlushStats {
        row_count: selected_count,
        min_seq: 0,
        max_seq,
        min_commit_seq: 0,
        max_commit_seq: max_seq,
    })
}

/// Resolves force-flush selection, preferring a small tombstone-only batch.
#[must_use]
pub fn resolve_force_flush_selection(
    all: FlushStats,
    delete_stats: FlushStats,
) -> ResolvedFlushSelection {
    if all.row_count == 0 {
        return ResolvedFlushSelection::new(all);
    }
    let delete_code = MirrorOperation::Delete.code();
    if delete_stats.row_count > 0 && delete_stats.row_count <= FORCE_TOMBSTONE_ONLY_CAP {
        return ResolvedFlushSelection {
            stats: delete_stats,
            mirror_ops: Some(vec![delete_code]),
        };
    }
    ResolvedFlushSelection::new(all)
}

/// Whether a catch-up wave should run for `selection` under a start-of-job watermark.
///
/// `catchup_upto_seq` is the mirror `max(seq)` observed when the job began. Waves
/// must not chase rows applied from concurrent WAL during flush fences — those
/// always receive higher seq values and wait for a later job.
#[must_use]
pub fn should_start_catchup_wave(
    catchup_upto_seq: Option<i64>,
    selection_row_count: i64,
    selection_min_seq: i64,
) -> bool {
    if selection_row_count <= 0 {
        return false;
    }
    match catchup_upto_seq {
        // No snapshot (empty mirror at claim): allow this selection, but the
        // caller must not loop after the wave.
        None => true,
        Some(upto) => selection_min_seq <= upto,
    }
}

/// Whether another catch-up wave is needed after flushing through `flushed_max_seq`.
#[must_use]
pub fn should_continue_flush_catchup(catchup_upto_seq: Option<i64>, flushed_max_seq: i64) -> bool {
    match catchup_upto_seq {
        None => false,
        Some(upto) => flushed_max_seq < upto,
    }
}

/// Caps a full-mirror force selection to one wave when a cutoff is available.
///
/// Tombstone-only selections and already-small mirrors are returned unchanged.
#[must_use]
pub fn apply_force_flush_wave_cap(
    selection: ResolvedFlushSelection,
    wave_cap: i64,
    cutoff: Option<(i64, i64)>,
) -> ResolvedFlushSelection {
    if selection.mirror_ops.is_some() || selection.stats.row_count <= wave_cap.max(0) {
        return selection;
    }
    let Some((selected_count, max_seq)) = cutoff else {
        return selection;
    };
    if selected_count <= 0 || max_seq <= 0 {
        return selection;
    }
    ResolvedFlushSelection::new(FlushStats {
        row_count: selected_count.min(selection.stats.row_count),
        min_seq: selection.stats.min_seq,
        max_seq,
        min_commit_seq: selection.stats.min_commit_seq,
        max_commit_seq: max_seq,
    })
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
