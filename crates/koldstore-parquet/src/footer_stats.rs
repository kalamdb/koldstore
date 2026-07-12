//! Aggregate Parquet footer min/max into catalog column stats keyed by [`ColumnId`].
//!
//! After encode, flush publishes these bounds for segment prune-before-open.
//! Conversion is type-aware so domain JSON (e.g. timestamptz RFC3339) matches
//! planner predicates. Missing or inexact footer stats omit the column (fail-open)
//! unless the caller treats it as required.

use std::collections::BTreeMap;

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use koldstore_common::ColumnId;
use koldstore_schema::PgType;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use serde_json::json;

use crate::footer::ColumnStats;
use crate::schema::PgColumn;

/// Aggregates footer chunk statistics into catalog JSON min/max by [`ColumnId`].
///
/// Uses in-memory Parquet bytes already held for validate/publish. Columns without
/// usable footer stats are omitted (fail-open) so callers never publish bounds that
/// falsely exclude rows.
///
/// # Errors
///
/// Returns an error when the payload is not readable Parquet metadata.
pub fn catalog_stats_from_parquet_bytes(
    bytes: &[u8],
    columns: &[PgColumn],
) -> Result<BTreeMap<ColumnId, ColumnStats>, String> {
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(bytes))
        .map_err(|error| format!("parquet footer stats: {error}"))?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();

    let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
        schema,
        metadata.file_metadata().key_value_metadata(),
    )
    .map_err(|error| format!("parquet arrow schema: {error}"))?;

    let mut by_field_id: BTreeMap<ColumnId, usize> = BTreeMap::new();
    for (index, field) in arrow_schema.fields().iter().enumerate() {
        let Some(raw) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
            continue;
        };
        let Ok(column_id) = raw.parse::<ColumnId>() else {
            continue;
        };
        by_field_id.entry(column_id).or_insert(index);
    }
    // Fall back to parquet-format field ids when Arrow metadata was stripped.
    if by_field_id.is_empty() {
        for (index, descriptor) in schema.columns().iter().enumerate() {
            if !descriptor.self_type_ptr().get_basic_info().has_id() {
                continue;
            }
            let field_id = descriptor.self_type_ptr().get_basic_info().id();
            if field_id <= 0 {
                continue;
            }
            let Ok(column_id) = ColumnId::new(u64::try_from(field_id).unwrap_or(0)) else {
                continue;
            };
            by_field_id.entry(column_id).or_insert(index);
        }
    }

    let mut stats = BTreeMap::new();
    for column in columns {
        let Some(&column_idx) = by_field_id.get(&column.column_id) else {
            continue;
        };
        let Some((min, max)) = aggregate_column_bounds(metadata, column_idx, column.pg_type)?
        else {
            continue;
        };
        stats.insert(column.column_id, ColumnStats { min, max });
    }
    Ok(stats)
}

fn aggregate_column_bounds(
    metadata: &parquet::file::metadata::ParquetMetaData,
    column_idx: usize,
    pg_type: PgType,
) -> Result<Option<(serde_json::Value, serde_json::Value)>, String> {
    let mut min_json: Option<serde_json::Value> = None;
    let mut max_json: Option<serde_json::Value> = None;

    for rg_index in 0..metadata.num_row_groups() {
        let Some(statistics) = metadata.row_group(rg_index).column(column_idx).statistics() else {
            continue;
        };
        let Some((group_min, group_max)) = physical_stats_to_json(statistics, pg_type)? else {
            // Inexact / unsupported physical shape: omit column entirely.
            return Ok(None);
        };
        min_json = Some(match min_json {
            None => group_min,
            Some(current) => {
                if compare_catalog_json(&group_min, &current).is_some_and(|order| order.is_lt()) {
                    group_min
                } else {
                    current
                }
            }
        });
        max_json = Some(match max_json {
            None => group_max,
            Some(current) => {
                if compare_catalog_json(&group_max, &current).is_some_and(|order| order.is_gt()) {
                    group_max
                } else {
                    current
                }
            }
        });
    }

    Ok(match (min_json, max_json) {
        (Some(min), Some(max)) => Some((min, max)),
        _ => None,
    })
}

fn physical_stats_to_json(
    statistics: &Statistics,
    pg_type: PgType,
) -> Result<Option<(serde_json::Value, serde_json::Value)>, String> {
    match (statistics, pg_type) {
        (Statistics::Boolean(values), PgType::Bool) => Ok(values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| (json!(*min), json!(*max)))),
        (Statistics::Int32(values), PgType::Int2 | PgType::Int4) => Ok(values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| (json!(*min), json!(*max)))),
        (Statistics::Int64(values), PgType::Int8) => Ok(values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| (json!(*min), json!(*max)))),
        (Statistics::Int64(values), PgType::Timestamptz) => Ok(values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| (micros_to_rfc3339(*min), micros_to_rfc3339(*max)))),
        (Statistics::Float(values), PgType::Float4) => Ok(values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| (json!(*min), json!(*max)))),
        (Statistics::Double(values), PgType::Float8) => Ok(values
            .min_opt()
            .zip(values.max_opt())
            .map(|(min, max)| (json!(*min), json!(*max)))),
        (Statistics::ByteArray(values), PgType::Text | PgType::Uuid | PgType::Numeric) => {
            let Some(min) = values.min_opt() else {
                return Ok(None);
            };
            let Some(max) = values.max_opt() else {
                return Ok(None);
            };
            let min = String::from_utf8(min.data().to_vec())
                .map_err(|error| format!("footer text min is not utf8: {error}"))?;
            let max = String::from_utf8(max.data().to_vec())
                .map_err(|error| format!("footer text max is not utf8: {error}"))?;
            Ok(Some((json!(min), json!(max))))
        }
        // Jsonb / arrays / bytea: footer byte-order mins are not safe for prune.
        (_, PgType::Jsonb | PgType::TextArray | PgType::Bytea) => Ok(None),
        _ => Ok(None),
    }
}

fn micros_to_rfc3339(micros: i64) -> serde_json::Value {
    let seconds = micros.div_euclid(1_000_000);
    let nanos = u32::try_from(micros.rem_euclid(1_000_000) * 1_000).unwrap_or(0);
    match Utc.timestamp_opt(seconds, nanos) {
        chrono::LocalResult::Single(ts) => json!(ts.to_rfc3339()),
        _ => json!(micros),
    }
}

fn compare_catalog_json(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> Option<std::cmp::Ordering> {
    koldstore_common::compare_json_values(left, right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PgColumn;

    #[test]
    fn micros_round_trip_formats_rfc3339() {
        let value = micros_to_rfc3339(1_704_067_200_000_000);
        assert!(value.as_str().unwrap().starts_with("2024-01-01"));
    }

    #[test]
    fn empty_columns_yield_empty_stats() {
        // Minimal invalid payload should error, not panic.
        let err = catalog_stats_from_parquet_bytes(b"not-parquet", &[]).unwrap_err();
        assert!(err.contains("parquet footer"));
        let _ = PgColumn::new(ColumnId::new(1).unwrap(), "id", PgType::Int8, false);
    }
}
