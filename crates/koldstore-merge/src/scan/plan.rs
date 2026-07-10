//! CustomScan plan serialization and PG-free pruning helpers.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use koldstore_common::{compare_json_values, KoldstoreError, Predicate, Result, ScopeKey, SeqId};
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

/// Validates that selected indexed predicates have segment min/max metadata.
///
/// # Errors
///
/// Returns an unsafe predicate error when any active segment lacks min/max stats
/// for a requested pruning column.
pub fn validate_prune_predicate_stats(
    segments: &[SegmentStatsHint],
    predicates: &[SegmentPrunePredicate],
) -> Result<()> {
    for predicate in predicates {
        for segment in segments {
            if !segment.column_stats.contains_key(&predicate.column) {
                return Err(KoldstoreError::UnsafePredicate(format!(
                    "cold filter column `{}` is indexed but segment `{}` has no min/max stats",
                    predicate.column, segment.object_path
                )));
            }
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
    if stats.min.is_null() || stats.max.is_null() {
        return true;
    }

    if let Some(min) = &predicate.min {
        if min.is_null() {
            return true;
        }
        match compare_json_values(&stats.max, min) {
            Some(Ordering::Less) => return false,
            Some(_) => {}
            None => return true,
        }
    }

    if let Some(max) = &predicate.max {
        if max.is_null() {
            return true;
        }
        match compare_json_values(&stats.min, max) {
            Some(Ordering::Greater) => return false,
            Some(_) => {}
            None => return true,
        }
    }

    true
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
