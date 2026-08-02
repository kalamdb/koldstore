//! CustomScan plan serialization and PG-free pruning helpers.

use std::collections::{BTreeMap, BTreeSet};

use koldstore_common::{KoldstoreError, Predicate, Result, ScopeKey, SeqId};
use serde::{Deserialize, Serialize};

/// Attribute numbers for merge metadata projected during hot/cold reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeMetadataAttnums {
    /// Mirror/cold `seq` attribute number.
    pub seq: i16,
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

/// Active cold segment metadata for merge reads (from catalog listing or index).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentStatsHint {
    /// Final object-store path.
    pub object_path: String,
    /// Schema version used to write this segment.
    pub schema_version: i32,
    /// Physical Parquet names for requested columns, keyed by stable ID.
    pub physical_names: BTreeMap<i16, String>,
    /// Object byte size when known (enables bounded footer range GETs on S3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_size: Option<u64>,
    /// Segment minimum `seq` (inclusive).
    pub min_seq: SeqId,
    /// Segment maximum `seq` (inclusive).
    pub max_seq: SeqId,
    /// Catalog-selected Parquet row groups; `None` means no packed prune ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_row_groups: Option<Vec<usize>>,
}

/// Min/max predicate proven safe for segment-level candidate pruning.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentPrunePredicate {
    /// Stable column ID whose segment stats should be checked.
    pub column_id: i16,
    /// Current column name for diagnostics and physical-name resolution.
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
    pub fn equality(column_id: i16, column: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            column_id,
            column: column.into(),
            min: Some(value.clone()),
            max: Some(value),
        }
    }

    /// Builds an inclusive range pruning predicate.
    #[must_use]
    pub fn closed_range(
        column_id: i16,
        column: impl Into<String>,
        min: serde_json::Value,
        max: serde_json::Value,
    ) -> Self {
        Self {
            column_id,
            column: column.into(),
            min: Some(min),
            max: Some(max),
        }
    }

    /// Builds a lower-bound pruning predicate.
    #[must_use]
    pub fn lower_bound(column_id: i16, column: impl Into<String>, min: serde_json::Value) -> Self {
        Self {
            column_id,
            column: column.into(),
            min: Some(min),
            max: None,
        }
    }

    /// Builds an upper-bound pruning predicate.
    #[must_use]
    pub fn upper_bound(column_id: i16, column: impl Into<String>, max: serde_json::Value) -> Self {
        Self {
            column_id,
            column: column.into(),
            min: None,
            max: Some(max),
        }
    }
}

/// Per-column policy for pre-merge cold segment prune via `cold_segment_index`.
///
/// Mutable application columns stay residual: pruning their newer cold version
/// can resurrect an older row. Scope and the configured segment order column are
/// safe because their values do not change across versions of a row.
///
/// Only Sort Key V1–allowlisted types participate in catalog index prune. Text
/// scope keys are residual and fall back to scanning all active segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColdPruneColumnPolicy {
    /// Column is part of the logical primary key.
    pub is_primary_key: bool,
    /// Column is the managed table `scope_column` (for example `tenant_id`).
    pub is_scope: bool,
    /// Column is the configured `segment_order_column_id`.
    pub is_order_column: bool,
    /// Column type is in the Sort Key V1 allowlist (`cold_segment_index`).
    pub sort_key_indexable: bool,
}

impl ColdPruneColumnPolicy {
    /// Whether `predicate` may prune segments before winner resolution.
    #[must_use]
    pub fn allows_predicate(self) -> bool {
        if !(self.is_primary_key || self.is_scope || self.is_order_column) {
            return false;
        }
        self.sort_key_indexable
    }
}

/// Keeps only pre-merge-safe cold prune predicates (PK + scope today).
///
/// `policy_for` returns [`None`] for unknown columns (dropped).
#[must_use]
pub fn retain_pre_merge_cold_prune_predicates(
    predicates: Vec<SegmentPrunePredicate>,
    mut policy_for: impl FnMut(i16) -> Option<ColdPruneColumnPolicy>,
) -> Vec<SegmentPrunePredicate> {
    predicates
        .into_iter()
        .filter(|predicate| {
            policy_for(predicate.column_id).is_some_and(ColdPruneColumnPolicy::allows_predicate)
        })
        .collect()
}

/// Validates that all cold pruning predicates target indexed/stat columns.
///
/// # Errors
///
/// Returns an unsafe predicate error when a filter references a column that was
/// not captured as an indexed cold-stat column.
pub fn validate_prune_predicates_indexed(
    predicates: &[SegmentPrunePredicate],
    indexed_column_ids: &[i16],
) -> Result<()> {
    let indexed_column_ids = indexed_column_ids.iter().copied().collect::<BTreeSet<_>>();
    for predicate in predicates {
        if !indexed_column_ids.contains(&predicate.column_id) {
            return Err(KoldstoreError::UnsafePredicate(format!(
                "cold filter column `{}` is not indexed; koldstore cold reads require WHERE filters on indexed columns",
                predicate.column
            )));
        }
    }
    Ok(())
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
/// Returns a catalog validation error when a segment has a reversed `seq` range.
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

/// Groups catalog segments into exact oldest-first cursor batches.
///
/// Disjoint sequence ranges remain separate so a bounded cursor reader can
/// stop before opening newer Parquet objects. Transitively overlapping ranges
/// stay together because their rows must be ordered and deduplicated as one
/// atomic batch.
///
/// # Errors
///
/// Returns a catalog validation error when a segment has a reversed sequence range.
pub fn group_segments_oldest_first(
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
        left.1
            .cmp(&right.1)
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.0.object_path.cmp(&right.0.object_path))
    });

    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_max = None::<SeqId>;
    for (segment, min, max) in ranged {
        if current_max.is_some_and(|group_max| min > group_max) {
            groups.push(std::mem::take(&mut current));
            current_max = None;
        }
        current_max = Some(current_max.map_or(max, |group_max| group_max.max(max)));
        current.push(segment);
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

fn segment_seq_range(segment: &SegmentStatsHint) -> Result<(SeqId, SeqId)> {
    if segment.min_seq > segment.max_seq {
        return Err(KoldstoreError::InvalidColdSegmentMetadata(format!(
            "cold segment `{}` has reversed `seq` range {}..={}",
            segment.object_path,
            segment.min_seq.get(),
            segment.max_seq.get()
        )));
    }
    Ok((segment.min_seq, segment.max_seq))
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
