//! Flush statistics and policy resolution helpers.

use koldstore_common::SeqId;
use koldstore_mirror::MirrorSeqStats;
use koldstore_parquet::CleanColdRecordPlan;

use crate::policy::{select_mirror_flush_candidates, FlushPolicy, MirrorPolicyRow};

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

impl From<MirrorSeqStats> for FlushStats {
    fn from(stats: MirrorSeqStats) -> Self {
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
}

/// Derives flush stats from planned cold records.
///
/// # Errors
///
/// Returns an error when a row is missing integer `_seq` metadata.
pub fn flush_stats_for_rows(rows: &[CleanColdRecordPlan]) -> Result<FlushStats, String> {
    let seqs = rows
        .iter()
        .map(|row| {
            row.values
                .get(koldstore_parquet::ColdMetadataColumn::Seq.name())
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "flush row is missing integer field `seq`".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let min_seq = *seqs.iter().min().expect("flush chunk is non-empty");
    let max_seq = *seqs.iter().max().expect("flush chunk is non-empty");
    Ok(FlushStats {
        row_count: i64::try_from(rows.len()).map_err(|error| error.to_string())?,
        min_seq,
        max_seq,
        min_commit_seq: min_seq,
        max_commit_seq: max_seq,
    })
}

/// Resolves flush stats from full mirror stats and optional flush policy.
#[must_use]
pub fn resolve_flush_stats(
    all: FlushStats,
    force: bool,
    policy: Option<&FlushPolicy>,
    policy_rows: &[MirrorPolicyRow],
) -> FlushStats {
    if all.row_count == 0 || force {
        return all;
    }
    let Some(policy) = policy else {
        return all;
    };
    let candidates = select_mirror_flush_candidates(policy, policy_rows);
    if candidates.is_empty() {
        return FlushStats::empty();
    }
    let seqs = candidates
        .iter()
        .map(|row| row.seq.get())
        .collect::<Vec<_>>();
    let min_seq = *seqs.iter().min().expect("flush candidates are non-empty");
    let max_seq = *seqs.iter().max().expect("flush candidates are non-empty");
    FlushStats {
        row_count: i64::try_from(candidates.len()).unwrap_or(i64::MAX),
        min_seq,
        max_seq,
        min_commit_seq: min_seq,
        max_commit_seq: max_seq,
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

/// Decodes mirror policy rows from a JSON payload.
///
/// # Errors
///
/// Returns an error when the payload is malformed.
pub fn decode_mirror_policy_rows(json: &str) -> Result<Vec<MirrorPolicyRow>, String> {
    use koldstore_mirror::MirrorPolicyRowJson;

    let values: Vec<MirrorPolicyRowJson> =
        serde_json::from_str(json).map_err(|error| error.to_string())?;
    values
        .into_iter()
        .map(|row| {
            Ok(MirrorPolicyRow {
                pk_json: row.pk_json,
                seq: SeqId::new(row.seq).map_err(|error| error.to_string())?,
            })
        })
        .collect()
}
