//! Row-group pruning helpers.
//!
//! Segment-level pruning lives in `koldstore-merge`. This module prunes
//! **row groups inside one Parquet file** using footer column stats and
//! native Parquet bloom filters (written on flush for PK columns).

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use bytes::Bytes;
use koldstore_common::{compare_json_values, CommitSeq, SeqId};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Type as ParquetPhysicalType;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::ChunkReader;
use parquet::file::statistics::Statistics;
use parquet::schema::types::SchemaDescriptor;

use crate::ColumnStats;

/// Pruning result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneDecision {
    pub selected_row_groups: Vec<usize>,
    pub skipped_row_groups: usize,
}

/// Row-group pruner placeholder.
#[derive(Debug, Default, Clone)]
pub struct RowGroupPruner;

impl RowGroupPruner {
    /// Prunes row groups whose `_seq` range cannot overlap the requested range.
    #[must_use]
    pub fn prune_seq_range(
        &self,
        footer: &crate::FooterSummary,
        min: SeqId,
        max: SeqId,
    ) -> PruneDecision {
        let selected_row_groups: Vec<usize> = footer
            .row_groups
            .iter()
            .filter(|row_group| match (row_group.min_seq, row_group.max_seq) {
                (Some(group_min), Some(group_max)) => {
                    group_max >= min.get() && group_min <= max.get()
                }
                _ => true,
            })
            .map(|row_group| row_group.row_group)
            .collect();
        PruneDecision {
            skipped_row_groups: footer
                .row_groups
                .len()
                .saturating_sub(selected_row_groups.len()),
            selected_row_groups,
        }
    }

    /// Prunes row groups whose `_commit_seq` range cannot overlap the requested range.
    #[must_use]
    pub fn prune_commit_seq_range(
        &self,
        footer: &crate::FooterSummary,
        min: CommitSeq,
        max: CommitSeq,
    ) -> PruneDecision {
        let selected_row_groups: Vec<usize> = footer
            .row_groups
            .iter()
            .filter(
                |row_group| match (row_group.min_commit_seq, row_group.max_commit_seq) {
                    (Some(group_min), Some(group_max)) => {
                        group_max >= min.get() && group_min <= max.get()
                    }
                    _ => true,
                },
            )
            .map(|row_group| row_group.row_group)
            .collect();

        PruneDecision {
            skipped_row_groups: footer
                .row_groups
                .len()
                .saturating_sub(selected_row_groups.len()),
            selected_row_groups,
        }
    }

    /// Prunes row groups using PK bloom/may-contain metadata.
    ///
    /// Row groups with no bloom metadata are selected because they cannot be
    /// proven irrelevant.
    #[must_use]
    pub fn prune_pk_values<I, S>(
        &self,
        footer: &crate::FooterSummary,
        bloom_values: &BTreeMap<usize, Vec<String>>,
        requested_values: I,
    ) -> PruneDecision
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let requested = requested_values
            .into_iter()
            .map(|value| value.as_ref().to_string())
            .collect::<Vec<_>>();
        let selected_row_groups = footer
            .row_groups
            .iter()
            .filter(|row_group| {
                bloom_values.get(&row_group.row_group).is_none_or(|values| {
                    values
                        .iter()
                        .any(|candidate| requested.iter().any(|requested| requested == candidate))
                })
            })
            .map(|row_group| row_group.row_group)
            .collect::<Vec<_>>();

        PruneDecision {
            skipped_row_groups: footer
                .row_groups
                .len()
                .saturating_sub(selected_row_groups.len()),
            selected_row_groups,
        }
    }

    /// Returns true when segment min/max stats may overlap the requested range.
    ///
    /// Missing, null, or incomparable stats return true so callers scan
    /// conservatively instead of risking false negatives.
    #[must_use]
    pub fn segment_column_may_overlap(
        &self,
        column_stats: &BTreeMap<String, ColumnStats>,
        column: &str,
        min: &serde_json::Value,
        max: &serde_json::Value,
    ) -> bool {
        let Some(stats) = column_stats.get(column) else {
            return true;
        };
        if min.is_null() || max.is_null() || stats.min.is_null() || stats.max.is_null() {
            return true;
        }
        let Some(max_vs_min) = compare_json_values(&stats.max, min) else {
            return true;
        };
        let Some(min_vs_max) = compare_json_values(&stats.min, max) else {
            return true;
        };

        max_vs_min != Ordering::Less && min_vs_max != Ordering::Greater
    }
}

/// Selects row groups that may contain any of the requested PK equality values.
///
/// Uses per-row-group column chunk min/max first. When multiple candidates
/// remain, native Parquet blooms refine the set via on-demand range reads on
/// the same reader (no second file open).
///
/// # Errors
///
/// Returns an error when the Parquet file cannot be opened or metadata is invalid.
pub fn select_row_groups_for_pk_values(
    path: impl AsRef<Path>,
    column: &str,
    values: &[String],
) -> Result<PruneDecision, String> {
    if values.is_empty() {
        return Ok(PruneDecision {
            selected_row_groups: Vec::new(),
            skipped_row_groups: 0,
        });
    }
    let file = File::open(path.as_ref()).map_err(|error| error.to_string())?;
    select_row_groups_for_pk_values_reader(file, column, values)
}

/// Selects row groups from an in-memory Parquet object.
///
/// # Errors
///
/// Returns an error when Parquet metadata or bloom filters are invalid.
pub fn select_row_groups_for_pk_values_bytes(
    bytes: Bytes,
    column: &str,
    values: &[String],
) -> Result<PruneDecision, String> {
    select_row_groups_for_pk_values_reader(bytes, column, values)
}

fn select_row_groups_for_pk_values_reader<R>(
    reader: R,
    column: &str,
    values: &[String],
) -> Result<PruneDecision, String>
where
    R: ChunkReader + 'static,
{
    if values.is_empty() {
        return Ok(PruneDecision {
            selected_row_groups: Vec::new(),
            skipped_row_groups: 0,
        });
    }

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|error| error.to_string())?;
    let metadata = builder.metadata();
    let schema = builder.parquet_schema();
    let column_index = column_index(schema, column)?;

    let mut stats_selected = Vec::new();
    let mut skipped_row_groups = 0usize;
    for rg_index in 0..metadata.num_row_groups() {
        let col = metadata.row_group(rg_index).column(column_index);
        if row_group_may_contain_pk_values(col.statistics(), values) {
            stats_selected.push(rg_index);
        } else {
            skipped_row_groups += 1;
        }
    }

    if stats_selected.len() <= 1 {
        return Ok(PruneDecision {
            selected_row_groups: stats_selected,
            skipped_row_groups,
        });
    }

    let physical_type = schema.column(column_index).physical_type();
    let mut selected_row_groups = Vec::new();
    for rg_index in stats_selected {
        match builder.get_row_group_column_bloom_filter(rg_index, column_index) {
            Ok(Some(bloom)) => {
                if values
                    .iter()
                    .any(|value| bloom_may_contain(&bloom, physical_type, value))
                {
                    selected_row_groups.push(rg_index);
                } else {
                    skipped_row_groups += 1;
                }
            }
            Ok(None) | Err(_) => selected_row_groups.push(rg_index),
        }
    }

    Ok(PruneDecision {
        selected_row_groups,
        skipped_row_groups,
    })
}

/// Min/max prune from already-loaded footer metadata (no I/O).
pub(crate) fn select_row_groups_from_metadata(
    metadata: &ParquetMetaData,
    schema: &SchemaDescriptor,
    column: &str,
    values: &[String],
) -> Result<(Vec<usize>, usize), String> {
    if values.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let column_index = column_index(schema, column)?;
    let mut selected = Vec::new();
    let mut skipped = 0usize;
    for rg_index in 0..metadata.num_row_groups() {
        let col = metadata.row_group(rg_index).column(column_index);
        if row_group_may_contain_pk_values(col.statistics(), values) {
            selected.push(rg_index);
        } else {
            skipped += 1;
        }
    }
    Ok((selected, skipped))
}

pub(crate) fn column_index(schema: &SchemaDescriptor, column: &str) -> Result<usize, String> {
    schema
        .columns()
        .iter()
        .position(|descr| descr.name() == column)
        .ok_or_else(|| format!("parquet schema missing PK column `{column}`"))
}

pub(crate) fn row_group_may_contain_pk_values(
    stats: Option<&Statistics>,
    values: &[String],
) -> bool {
    let Some(stats) = stats else {
        return true;
    };
    match stats {
        Statistics::Int64(typed) => {
            let (Some(min), Some(max)) = (typed.min_opt(), typed.max_opt()) else {
                return true;
            };
            values.iter().any(|value| {
                value
                    .parse::<i64>()
                    .is_ok_and(|parsed| parsed >= *min && parsed <= *max)
            })
        }
        Statistics::Int32(typed) => {
            let (Some(min), Some(max)) = (typed.min_opt(), typed.max_opt()) else {
                return true;
            };
            values.iter().any(|value| {
                value
                    .parse::<i32>()
                    .is_ok_and(|parsed| parsed >= *min && parsed <= *max)
            })
        }
        Statistics::ByteArray(typed) => {
            let (Some(min), Some(max)) = (typed.min_opt(), typed.max_opt()) else {
                return true;
            };
            let min_bytes = min.data();
            let max_bytes = max.data();
            values.iter().any(|value| {
                let bytes = value.as_bytes();
                bytes >= min_bytes && bytes <= max_bytes
            })
        }
        _ => true,
    }
}

pub(crate) fn bloom_may_contain(
    bloom: &Sbbf,
    physical_type: ParquetPhysicalType,
    value: &str,
) -> bool {
    match physical_type {
        ParquetPhysicalType::INT32 => value.parse::<i32>().map_or(true, |v| bloom.check(&v)),
        ParquetPhysicalType::INT64 => value.parse::<i64>().map_or(true, |v| bloom.check(&v)),
        ParquetPhysicalType::BYTE_ARRAY | ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY => {
            bloom.check(value)
        }
        ParquetPhysicalType::BOOLEAN => value.parse::<bool>().map_or(true, |v| bloom.check(&v)),
        ParquetPhysicalType::FLOAT => value.parse::<f32>().map_or(true, |v| bloom.check(&v)),
        ParquetPhysicalType::DOUBLE => value.parse::<f64>().map_or(true, |v| bloom.check(&v)),
        ParquetPhysicalType::INT96 => true,
    }
}
