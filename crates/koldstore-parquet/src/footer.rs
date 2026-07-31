//! Parquet footer summaries and packed catalog metadata extraction.

use std::collections::BTreeMap;

use koldstore_common::{ColumnId, ColumnRef};
use koldstore_sortkey::{
    encode_sort_key, encode_sort_key_pg_text, SortKeyType, SortKeyValue, CODEC_VERSION,
    PG_EPOCH_DAYS_FROM_UNIX, PG_EPOCH_MICROS_FROM_UNIX,
};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use serde::{Deserialize, Serialize};

use crate::schema::{ColdMetadataColumn, PgColumn};

type PackedSortKeyBounds = (Vec<u8>, Vec<u8>);

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
    /// Segment row count.
    pub row_count: u64,
    /// Final object byte size.
    pub byte_size: u64,
    /// Schema version written into the segment.
    pub schema_version: u32,
    /// Column stats used for manifest and local pruning metadata.
    pub column_stats: BTreeMap<String, ColumnStats>,
}

/// Packed row-group bounds for one safely indexable application column.
///
/// Every row-group vector is positionally aligned: index `n` describes Parquet
/// row group `n`. A missing bound means either a proven all-null group or
/// unavailable statistics; [`Self::row_group_null_counts`] distinguishes them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackedColumnIndex {
    /// Stable PostgreSQL attribute number.
    pub column_id: ColumnId,
    /// PostgreSQL type OID used to interpret Sort Key V1 bytes.
    pub type_oid: u32,
    /// Persisted Sort Key codec version.
    pub codec_version: i16,
    /// Inclusive segment lower bound when every value-bearing row group is known.
    pub min_value: Option<Vec<u8>>,
    /// Inclusive segment upper bound when every value-bearing row group is known.
    pub max_value: Option<Vec<u8>>,
    /// Per-row-group inclusive lower bounds.
    pub row_group_min_values: Vec<Option<Vec<u8>>>,
    /// Per-row-group inclusive upper bounds.
    pub row_group_max_values: Vec<Option<Vec<u8>>>,
    /// Per-row-group null counts; `None` means the footer did not report one.
    pub row_group_null_counts: Vec<Option<i64>>,
}

/// Footer-derived metadata persisted beside one immutable cold segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackedSegmentMetadata {
    /// Total logical rows reported by the finalized footer.
    pub row_count: i64,
    /// Inclusive segment minimum SeqId derived from row-group minima.
    pub min_seq: i64,
    /// Inclusive segment maximum SeqId derived from row-group maxima.
    pub max_seq: i64,
    /// Number of non-empty Parquet row groups.
    pub row_group_count: usize,
    /// Logical rows in each row group.
    pub row_group_row_counts: Vec<i64>,
    /// Required minimum SeqId for each row group.
    pub row_group_min_seqs: Vec<i64>,
    /// Required maximum SeqId for each row group.
    pub row_group_max_seqs: Vec<i64>,
    /// Packed bounds for supported indexed application columns.
    pub column_indexes: Vec<PackedColumnIndex>,
}

/// Extracts compact catalog metadata from the finalized Parquet footer.
///
/// Supported indexed columns are encoded directly into Sort Key V1 bytes.
/// Unsupported types are omitted. Required non-null primary-key columns and
/// the internal SeqId column must have exact min/max statistics in every row
/// group or extraction fails.
///
/// # Errors
///
/// Returns an error for an empty file, missing/malformed SeqId statistics,
/// missing indexed catalog columns, physical-type mismatches, or incomplete
/// required primary-key statistics.
pub fn extract_packed_segment_metadata(
    metadata: &ParquetMetaData,
    columns: &[PgColumn],
    indexed_columns: &[ColumnRef],
    primary_key_columns: &[String],
) -> Result<PackedSegmentMetadata, String> {
    if metadata.num_row_groups() == 0 {
        return Err("cold segment footer contains no row groups".to_string());
    }

    let seq_column_index = parquet_column_index(metadata, ColdMetadataColumn::Seq.name())
        .ok_or_else(|| "cold segment footer is missing required `seq` column".to_string())?;
    let mut row_group_row_counts = Vec::with_capacity(metadata.num_row_groups());
    let mut row_group_min_seqs = Vec::with_capacity(metadata.num_row_groups());
    let mut row_group_max_seqs = Vec::with_capacity(metadata.num_row_groups());
    for (row_group_id, row_group) in metadata.row_groups().iter().enumerate() {
        let row_count = row_group.num_rows();
        if row_count <= 0 {
            return Err(format!(
                "cold segment row group {row_group_id} has non-positive row count {row_count}"
            ));
        }
        let statistics = row_group
            .column(seq_column_index)
            .statistics()
            .ok_or_else(|| {
                format!(
                    "cold segment row group {row_group_id} is missing required `seq` statistics"
                )
            })?;
        let (min_seq, max_seq) = exact_i64_bounds(statistics).ok_or_else(|| {
            format!(
                "cold segment row group {row_group_id} has incomplete required `seq` statistics"
            )
        })?;
        if min_seq <= 0 || min_seq > max_seq {
            return Err(format!(
                "cold segment row group {row_group_id} has invalid `seq` bounds {min_seq}..={max_seq}"
            ));
        }
        row_group_row_counts.push(row_count);
        row_group_min_seqs.push(min_seq);
        row_group_max_seqs.push(max_seq);
    }

    let mut column_indexes = Vec::with_capacity(indexed_columns.len());
    for indexed_column in indexed_columns {
        let column = columns
            .iter()
            .find(|column| column.name == indexed_column.name)
            .ok_or_else(|| {
                format!(
                    "indexed column `{}` is missing from Parquet catalog columns",
                    indexed_column.name
                )
            })?;
        let type_oid = column.pg_type.type_oid();
        let Some(sort_key_type) = SortKeyType::from_type_oid(type_oid) else {
            continue;
        };
        let column_index = parquet_column_index(metadata, &column.name).ok_or_else(|| {
            format!(
                "cold segment footer is missing indexed column `{}`",
                column.name
            )
        })?;
        let required = !column.nullable
            && primary_key_columns
                .iter()
                .any(|primary_key| primary_key == &column.name);
        column_indexes.push(extract_column_index(
            metadata,
            column_index,
            indexed_column.column_id,
            type_oid,
            sort_key_type,
            required,
            &column.name,
            &row_group_row_counts,
        )?);
    }

    let row_count = row_group_row_counts
        .iter()
        .try_fold(0_i64, |total, count| {
            total
                .checked_add(*count)
                .ok_or_else(|| "cold segment footer row count exceeds bigint range".to_string())
        })?;
    let min_seq = row_group_min_seqs
        .iter()
        .copied()
        .min()
        .expect("non-empty row-group metadata");
    let max_seq = row_group_max_seqs
        .iter()
        .copied()
        .max()
        .expect("non-empty row-group metadata");

    Ok(PackedSegmentMetadata {
        row_count,
        min_seq,
        max_seq,
        row_group_count: metadata.num_row_groups(),
        row_group_row_counts,
        row_group_min_seqs,
        row_group_max_seqs,
        column_indexes,
    })
}

#[allow(clippy::too_many_arguments)]
fn extract_column_index(
    metadata: &ParquetMetaData,
    column_index: usize,
    column_id: ColumnId,
    type_oid: u32,
    sort_key_type: SortKeyType,
    required: bool,
    column_name: &str,
    row_group_row_counts: &[i64],
) -> Result<PackedColumnIndex, String> {
    let mut row_group_min_values = Vec::with_capacity(metadata.num_row_groups());
    let mut row_group_max_values = Vec::with_capacity(metadata.num_row_groups());
    let mut row_group_null_counts = Vec::with_capacity(metadata.num_row_groups());
    let mut segment_bounds_complete = true;

    for (row_group_id, row_group) in metadata.row_groups().iter().enumerate() {
        let statistics = row_group.column(column_index).statistics();
        let null_count = statistics
            .and_then(Statistics::null_count_opt)
            .map(|count| i64::try_from(count).map_err(|error| error.to_string()))
            .transpose()?;
        if null_count.is_some_and(|count| count < 0 || count > row_group_row_counts[row_group_id]) {
            return Err(format!(
                "cold segment row group {row_group_id} has invalid `{column_name}` null count"
            ));
        }
        let bounds = statistics
            .map(|statistics| exact_sort_key_bounds(sort_key_type, statistics))
            .transpose()?
            .flatten();
        let all_null = null_count == Some(row_group_row_counts[row_group_id]);
        if all_null && bounds.is_some() {
            return Err(format!(
                "cold segment row group {row_group_id} has `{column_name}` bounds for an all-null column"
            ));
        }
        if bounds.is_none() && !all_null {
            segment_bounds_complete = false;
            if required {
                return Err(format!(
                    "cold segment row group {row_group_id} has incomplete required `{column_name}` statistics"
                ));
            }
        }
        let (min_value, max_value) = bounds.unzip();
        row_group_min_values.push(min_value);
        row_group_max_values.push(max_value);
        row_group_null_counts.push(null_count);
    }

    let min_value = segment_bounds_complete
        .then(|| row_group_min_values.iter().filter_map(Clone::clone).min())
        .flatten();
    let max_value = segment_bounds_complete
        .then(|| row_group_max_values.iter().filter_map(Clone::clone).max())
        .flatten();

    Ok(PackedColumnIndex {
        column_id,
        type_oid,
        codec_version: CODEC_VERSION,
        min_value,
        max_value,
        row_group_min_values,
        row_group_max_values,
        row_group_null_counts,
    })
}

fn parquet_column_index(metadata: &ParquetMetaData, column_name: &str) -> Option<usize> {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.name() == column_name)
}

fn exact_i64_bounds(statistics: &Statistics) -> Option<(i64, i64)> {
    if !statistics.min_is_exact() || !statistics.max_is_exact() {
        return None;
    }
    match statistics {
        Statistics::Int64(values) => values.min_opt().copied().zip(values.max_opt().copied()),
        _ => None,
    }
}

fn exact_sort_key_bounds(
    sort_key_type: SortKeyType,
    statistics: &Statistics,
) -> Result<Option<PackedSortKeyBounds>, String> {
    if !statistics.min_is_exact() || !statistics.max_is_exact() {
        return Ok(None);
    }
    let values = match (sort_key_type, statistics) {
        (SortKeyType::Bool, Statistics::Boolean(values)) => values
            .min_opt()
            .copied()
            .zip(values.max_opt().copied())
            .map(|(min, max)| (SortKeyValue::Bool(min), SortKeyValue::Bool(max))),
        (SortKeyType::Int2, Statistics::Int32(values)) => values
            .min_opt()
            .copied()
            .zip(values.max_opt().copied())
            .map(|(min, max)| -> Result<_, String> {
                Ok((
                    SortKeyValue::Int2(i16::try_from(min).map_err(|error| error.to_string())?),
                    SortKeyValue::Int2(i16::try_from(max).map_err(|error| error.to_string())?),
                ))
            })
            .transpose()?,
        (SortKeyType::Int4, Statistics::Int32(values)) => values
            .min_opt()
            .copied()
            .zip(values.max_opt().copied())
            .map(|(min, max)| (SortKeyValue::Int4(min), SortKeyValue::Int4(max))),
        (SortKeyType::Int8, Statistics::Int64(values)) => values
            .min_opt()
            .copied()
            .zip(values.max_opt().copied())
            .map(|(min, max)| (SortKeyValue::Int8(min), SortKeyValue::Int8(max))),
        (SortKeyType::Date, Statistics::Int32(values)) => values
            .min_opt()
            .copied()
            .zip(values.max_opt().copied())
            .map(|(min, max)| {
                (
                    SortKeyValue::Date(min.saturating_sub(PG_EPOCH_DAYS_FROM_UNIX)),
                    SortKeyValue::Date(max.saturating_sub(PG_EPOCH_DAYS_FROM_UNIX)),
                )
            }),
        (SortKeyType::Timestamp, Statistics::Int64(values)) => values
            .min_opt()
            .copied()
            .zip(values.max_opt().copied())
            .map(|(min, max)| {
                (
                    SortKeyValue::Timestamp(min.saturating_sub(PG_EPOCH_MICROS_FROM_UNIX)),
                    SortKeyValue::Timestamp(max.saturating_sub(PG_EPOCH_MICROS_FROM_UNIX)),
                )
            }),
        (SortKeyType::Timestamptz, Statistics::Int64(values)) => values
            .min_opt()
            .copied()
            .zip(values.max_opt().copied())
            .map(|(min, max)| {
                (
                    SortKeyValue::Timestamptz(min.saturating_sub(PG_EPOCH_MICROS_FROM_UNIX)),
                    SortKeyValue::Timestamptz(max.saturating_sub(PG_EPOCH_MICROS_FROM_UNIX)),
                )
            }),
        (SortKeyType::Uuid, Statistics::ByteArray(values)) => {
            let Some((min, max)) = values.min_opt().zip(values.max_opt()) else {
                return Ok(None);
            };
            let min = std::str::from_utf8(min.data())
                .map_err(|error| format!("invalid UUID footer minimum: {error}"))?;
            let max = std::str::from_utf8(max.data())
                .map_err(|error| format!("invalid UUID footer maximum: {error}"))?;
            return Ok(Some((
                encode_sort_key_pg_text(SortKeyType::Uuid, min)
                    .map_err(|error| error.to_string())?,
                encode_sort_key_pg_text(SortKeyType::Uuid, max)
                    .map_err(|error| error.to_string())?,
            )));
        }
        _ => {
            return Err(format!(
                "Parquet statistics physical type does not match Sort Key type {sort_key_type:?}"
            ));
        }
    };

    values
        .map(|(min, max)| {
            Ok((
                encode_sort_key(&min).map_err(|error| error.to_string())?,
                encode_sort_key(&max).map_err(|error| error.to_string())?,
            ))
        })
        .transpose()
}

impl FooterSummary {
    /// Returns segment-level sequence and commit bounds from row groups.
    #[must_use]
    pub fn segment_bounds(&self) -> Option<(i64, i64)> {
        let min_seq = self.row_groups.iter().filter_map(|rg| rg.min_seq).min()?;
        let max_seq = self.row_groups.iter().filter_map(|rg| rg.max_seq).max()?;
        Some((min_seq, max_seq))
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
        let (min_seq, max_seq) = footer.segment_bounds()?;

        Some(Self {
            min_seq,
            max_seq,
            row_count,
            byte_size,
            schema_version,
            column_stats: column_stats.into_iter().collect(),
        })
    }
}
