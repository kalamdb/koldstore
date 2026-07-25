//! CustomScan plan serialization and PG-free pruning helpers.

use std::collections::{BTreeMap, BTreeSet};

use koldstore_common::{
    column_stats_range_may_overlap, KoldstoreError, Predicate, Result, ScopeKey, SeqId,
};
use serde::{Deserialize, Serialize};

/// Attribute numbers for merge metadata projected during hot/cold reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeMetadataAttnums {
    /// Mirror/cold `seq` attribute number.
    pub seq: i16,
    /// Commit-order cursor attribute number.
    pub commit_seq: i16,
    /// Delete/tombstone attribute number.
    pub deleted: i16,
    /// Optional scope attribute number.
    pub scope: Option<i16>,
}

/// Cold segment hint serialized into the CustomScan plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentHint {
    /// Segment catalog identity.
    pub segment_id: String,
    /// Optional user scope for the cold segment.
    pub scope_key: Option<ScopeKey>,
    /// Final object-store path.
    pub object_path: String,
    /// Selected row groups after safe pruning.
    pub selected_row_groups: Vec<usize>,
    /// Segment minimum `seq`.
    pub min_seq: SeqId,
    /// Segment maximum `seq`.
    pub max_seq: SeqId,
}

/// Segment stats loaded from the manifest-backed cold segment catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentStatsHint {
    /// Final object-store path.
    pub object_path: String,
    /// Segment-level min/max stats by column.
    pub column_stats: BTreeMap<String, koldstore_parquet::ColumnStats>,
    /// Object byte size when known (enables bounded footer range GETs on S3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_size: Option<u64>,
}

/// Min/max predicate proven safe for segment-level candidate pruning.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentPrunePredicate {
    /// Column whose segment stats should be checked.
    pub column: String,
    /// Inclusive lower bound, when present.
    pub min: Option<serde_json::Value>,
    /// Inclusive upper bound, when present.
    pub max: Option<serde_json::Value>,
}

impl SegmentPrunePredicate {
    /// True when this predicate is a point equality (`min == max`).
    #[must_use]
    pub fn is_equality(&self) -> bool {
        match (&self.min, &self.max) {
            (Some(min), Some(max)) => min == max,
            _ => false,
        }
    }

    /// Builds an equality pruning predicate.
    #[must_use]
    pub fn equality(column: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            column: column.into(),
            min: Some(value.clone()),
            max: Some(value),
        }
    }

    /// Builds an inclusive range pruning predicate.
    #[must_use]
    pub fn closed_range(
        column: impl Into<String>,
        min: serde_json::Value,
        max: serde_json::Value,
    ) -> Self {
        Self {
            column: column.into(),
            min: Some(min),
            max: Some(max),
        }
    }

    /// Builds a lower-bound pruning predicate.
    #[must_use]
    pub fn lower_bound(column: impl Into<String>, min: serde_json::Value) -> Self {
        Self {
            column: column.into(),
            min: Some(min),
            max: None,
        }
    }

    /// Builds an upper-bound pruning predicate.
    #[must_use]
    pub fn upper_bound(column: impl Into<String>, max: serde_json::Value) -> Self {
        Self {
            column: column.into(),
            min: None,
            max: Some(max),
        }
    }
}

/// Per-column policy for pre-merge cold segment prune via catalog min/max.
///
/// Mutable application columns stay residual: pruning their newer cold version
/// can resurrect an older row. Scope is safe because the scope key does not
/// change across versions of a row (RLS/user identity). Version cursors
/// (`seq` / `commit_seq`) identify specific versions and are safe to prune.
///
/// Today all active segments live under the shared catalog manifest
/// (`scope_key = ''`); scope is treated like an indexed stats column. Later
/// each `scope_id` will own its own `manifest.json` + folder, and listing will
/// filter by `scope_key` first — min/max remains a secondary prune inside that
/// scope's segment set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColdPruneColumnPolicy {
    /// Column is part of the logical primary key.
    pub is_primary_key: bool,
    /// Column is the managed table `scope_column` (for example `tenant_id`).
    pub is_scope: bool,
    /// Column is a version cursor (`seq` / `commit_seq`).
    pub is_version_cursor: bool,
    /// Min/max ordering matches PostgreSQL (int, bool, uuid, …).
    pub ordered_stats_safe: bool,
    /// Exact equality against catalog JSON encoding is safe (text scope ids).
    pub equality_stats_safe: bool,
}

impl ColdPruneColumnPolicy {
    /// Whether `predicate` may prune segments before winner resolution.
    #[must_use]
    pub fn allows_predicate(self, predicate: &SegmentPrunePredicate) -> bool {
        if self.is_primary_key || self.is_version_cursor {
            return self.ordered_stats_safe;
        }
        if self.is_scope {
            if self.ordered_stats_safe {
                return true;
            }
            // Text scope keys: equality-only against flush-encoded JSON stats.
            return self.equality_stats_safe && predicate.is_equality();
        }
        false
    }
}

/// Keeps only pre-merge-safe cold prune predicates (PK + scope + version cursors).
///
/// `policy_for` returns [`None`] for unknown columns (dropped).
#[must_use]
pub fn retain_pre_merge_cold_prune_predicates(
    predicates: Vec<SegmentPrunePredicate>,
    mut policy_for: impl FnMut(&str) -> Option<ColdPruneColumnPolicy>,
) -> Vec<SegmentPrunePredicate> {
    predicates
        .into_iter()
        .filter(|predicate| {
            policy_for(predicate.column.as_str())
                .is_some_and(|policy| policy.allows_predicate(predicate))
        })
        .collect()
}

/// Returns segment paths whose manifest min/max stats cannot prove non-overlap.
///
/// Missing or incomparable stats keep the segment selected. The SQL executor
/// still applies residual quals after winner resolution; this only avoids
/// opening Parquet files that cannot contain a candidate row.
#[must_use]
pub fn prune_segment_stats(
    segments: &[SegmentStatsHint],
    predicates: &[SegmentPrunePredicate],
) -> Vec<String> {
    prune_segment_stats_hints(segments, predicates)
        .into_iter()
        .map(|segment| segment.object_path)
        .collect()
}

/// Like [`prune_segment_stats`], but keeps full segment hints (including
/// `byte_size` for footer-bounded ObjectStore reads).
#[must_use]
pub fn prune_segment_stats_hints(
    segments: &[SegmentStatsHint],
    predicates: &[SegmentPrunePredicate],
) -> Vec<SegmentStatsHint> {
    segments
        .iter()
        .filter(|segment| {
            predicates
                .iter()
                .all(|predicate| segment_may_match_predicate(segment, predicate))
        })
        .cloned()
        .collect()
}

/// Groups catalog segments into exact newest-first merge batches.
///
/// Disjoint sequence ranges remain separate so the executor can drop one
/// segment payload before opening the next. Transitively overlapping ranges
/// stay together because winner resolution cannot safely emit either range
/// until all overlapping candidates have been compared.
///
/// # Errors
///
/// Returns a catalog validation error when a segment lacks a valid closed
/// `seq` range.
pub fn group_segments_newest_first(
    segments: Vec<SegmentStatsHint>,
) -> Result<Vec<Vec<SegmentStatsHint>>> {
    let mut ranged = segments
        .into_iter()
        .map(|segment| {
            let (min, max) = segment_seq_range(&segment)?;
            Ok((segment, min, max))
        })
        .collect::<Result<Vec<_>>>()?;
    ranged.sort_by(|left, right| {
        right
            .2
            .cmp(&left.2)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.0.object_path.cmp(&right.0.object_path))
    });

    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_min = None::<SeqId>;
    for (segment, min, max) in ranged {
        if current_min.is_some_and(|group_min| max < group_min) {
            groups.push(std::mem::take(&mut current));
            current_min = None;
        }
        current_min = Some(current_min.map_or(min, |group_min| group_min.min(min)));
        current.push(segment);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

fn segment_seq_range(segment: &SegmentStatsHint) -> Result<(SeqId, SeqId)> {
    let invalid = KoldstoreError::InvalidColdSegmentMetadata;
    let stats = segment.column_stats.get("seq").ok_or_else(|| {
        invalid(format!(
            "cold segment `{}` is missing required `seq` statistics",
            segment.object_path
        ))
    })?;
    let min_raw = json_i64(&stats.min).ok_or_else(|| {
        invalid(format!(
            "cold segment `{}` has invalid minimum `seq` statistic",
            segment.object_path
        ))
    })?;
    let max_raw = json_i64(&stats.max).ok_or_else(|| {
        invalid(format!(
            "cold segment `{}` has invalid maximum `seq` statistic",
            segment.object_path
        ))
    })?;
    let min = SeqId::new(min_raw)?;
    let max = SeqId::new(max_raw)?;
    if min > max {
        return Err(invalid(format!(
            "cold segment `{}` has reversed `seq` range {min_raw}..={max_raw}",
            segment.object_path
        )));
    }
    Ok((min, max))
}

fn json_i64(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(number) => number.as_i64(),
        serde_json::Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

/// Validates that all cold pruning predicates target indexed/stat columns.
///
/// # Errors
///
/// Returns an unsafe predicate error when a filter references a column that was
/// not captured as an indexed cold-stat column.
pub fn validate_prune_predicates_indexed(
    predicates: &[SegmentPrunePredicate],
    indexed_columns: &[String],
) -> Result<()> {
    let indexed_columns = indexed_columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for predicate in predicates {
        if !indexed_columns.contains(predicate.column.as_str()) {
            return Err(KoldstoreError::UnsafePredicate(format!(
                "cold filter column `{}` is not indexed; koldstore cold reads require WHERE filters on indexed columns",
                predicate.column
            )));
        }
    }
    Ok(())
}

fn segment_may_match_predicate(
    segment: &SegmentStatsHint,
    predicate: &SegmentPrunePredicate,
) -> bool {
    let Some(stats) = segment.column_stats.get(&predicate.column) else {
        return true;
    };
    column_stats_range_may_overlap(
        &stats.min,
        &stats.max,
        predicate.min.as_ref(),
        predicate.max.as_ref(),
    )
}

/// How unflushed mirror rows participate in merge reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MirrorOverlayStrategy {
    /// Mask cold rows whose PK appears in the mirror (op 1/2/3).
    #[default]
    MirrorMask,
}

/// Serialized custom-plan identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeScanPlan {
    /// Managed table oid.
    pub table_oid: u32,
    /// PostgreSQL scan relation id.
    pub scanrelid: u32,
    /// Logical primary-key columns.
    pub primary_key_columns: Vec<String>,
    /// Merge metadata attnums projected by the scan.
    pub merge_metadata_attnums: MergeMetadataAttnums,
    /// Optional user scope key captured at planning time.
    pub scope_key: Option<ScopeKey>,
    /// Predicates proven safe for pre-merge pruning.
    pub safe_quals: Vec<Predicate>,
    /// Residual predicates evaluated after winner resolution.
    pub residual_quals: Vec<Predicate>,
    /// Security/RLS predicates evaluated after winner resolution or fail-closed.
    pub security_quals: Vec<Predicate>,
    /// Required output/qual columns.
    pub projection: Vec<String>,
    /// Visible cold segment hints.
    ///
    /// Live CustomScan planning leaves this empty and prunes catalog segments at
    /// execution time via [`prune_segment_stats_hints`]. Populated only by
    /// library helpers / tests that drive [`crate::scan::begin_merge_scan_with_plan`].
    pub segment_hints: Vec<SegmentHint>,
    /// Mirror overlay strategy applied at execution.
    #[serde(default)]
    pub overlay_strategy: MirrorOverlayStrategy,
}

impl MergeScanPlan {
    /// Creates a merge scan plan.
    #[must_use]
    pub fn new(table_oid: u32, primary_key_columns: Vec<String>) -> Self {
        Self {
            table_oid,
            scanrelid: 0,
            primary_key_columns,
            merge_metadata_attnums: MergeMetadataAttnums {
                seq: 0,
                commit_seq: 0,
                deleted: 0,
                scope: None,
            },
            scope_key: None,
            safe_quals: Vec::new(),
            residual_quals: Vec::new(),
            security_quals: Vec::new(),
            projection: Vec::new(),
            segment_hints: Vec::new(),
            overlay_strategy: MirrorOverlayStrategy::MirrorMask,
        }
    }

    /// Serializes the plan payload for PostgreSQL `custom_private`.
    ///
    /// # Errors
    ///
    /// Returns a JSON error if the payload cannot be serialized.
    pub fn serialize(&self) -> Result<String> {
        serde_json::to_string(self).map_err(Into::into)
    }

    /// Deserializes a plan payload from PostgreSQL `custom_private`.
    ///
    /// # Errors
    ///
    /// Returns a JSON error if the payload is malformed.
    pub fn deserialize(value: &str) -> Result<Self> {
        serde_json::from_str(value).map_err(Into::into)
    }

    /// Expressions that PostgreSQL must evaluate after winner resolution.
    #[must_use]
    pub fn custom_exprs(&self) -> Vec<Predicate> {
        self.residual_quals
            .iter()
            .chain(self.security_quals.iter())
            .cloned()
            .collect()
    }

    /// Projection columns serialized into `custom_private`.
    #[must_use]
    pub fn custom_private_projection(&self) -> &[String] {
        &self.projection
    }
}
