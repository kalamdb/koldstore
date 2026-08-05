//! Decode Arrow record batches into clean cold rows.

use arrow_array::{Array, BooleanArray, Int16Array, Int64Array, RecordBatch, UInt32Array};
use koldstore_common::{ColdRow, LogicalPk, PkColumn, RowImage, SeqId};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use crate::schema::{ColdMetadataColumn, PgColumn};

use super::options::{ParquetReadOptions, PkValues};
use super::types::CleanColdRow;

fn arrow_cell_matches_pk_values(array: &dyn Array, row_index: usize, values: &[String]) -> bool {
    if array.is_null(row_index) {
        return false;
    }
    if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        let actual = ints.value(row_index);
        return values
            .iter()
            .any(|expected| expected.parse::<i64>().is_ok_and(|parsed| parsed == actual));
    }
    if let Some(ints) = array.as_any().downcast_ref::<arrow_array::Int32Array>() {
        let actual = ints.value(row_index);
        return values
            .iter()
            .any(|expected| expected.parse::<i32>().is_ok_and(|parsed| parsed == actual));
    }
    if let Some(texts) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
        let actual = texts.value(row_index);
        return values.iter().any(|expected| expected == actual);
    }
    false
}

/// Converts a clean-schema parquet row into the shared [`ColdRow`] model.
///
/// # Errors
///
/// Returns an error when primary-key columns are invalid or sequence values are non-positive.
pub fn clean_cold_row_to_common(
    row: CleanColdRow,
    pk_columns: &[String],
) -> Result<ColdRow, String> {
    let ordered_pk_columns: Vec<PkColumn> = pk_columns
        .iter()
        .map(|name| PkColumn::new(name).map_err(|error| error.to_string()))
        .collect::<Result<_, _>>()?;
    let pk = LogicalPk::from_json_object(&row.pk_json, &ordered_pk_columns)
        .map_err(|error| error.to_string())?;
    Ok(ColdRow {
        pk,
        scope_key: None,
        seq: SeqId::new(row.seq).map_err(|error| error.to_string())?,
        deleted: row.deleted,
        schema_version: row.schema_version,
        row_image: row.row_image,
    })
}

pub(super) fn application_columns_for_read(
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<Vec<String>, String> {
    if options.columns.is_empty() {
        return Ok(columns.iter().map(|column| column.name.clone()).collect());
    }
    let application: Vec<String> = options
        .columns
        .iter()
        .filter(|column| !is_clean_metadata_column(column))
        .cloned()
        .collect();
    for pk in primary_key_columns {
        if !application.iter().any(|column| column == pk) {
            return Err(format!(
                "parquet read projection is missing required primary-key column `{pk}`"
            ));
        }
    }
    Ok(application)
}

pub(super) fn projection_mask(
    schema: &SchemaDescriptor,
    application_columns: &[String],
) -> ProjectionMask {
    let mut names = vec![
        ColdMetadataColumn::Seq.name(),
        ColdMetadataColumn::Op.name(),
        ColdMetadataColumn::Deleted.name(),
        ColdMetadataColumn::SchemaVersion.name(),
    ];
    for column in application_columns {
        if is_clean_metadata_column(column) {
            continue;
        }
        if !names.iter().any(|name| name == column) {
            names.push(column.as_str());
        }
    }
    ProjectionMask::columns(schema, names)
}

fn is_clean_metadata_column(name: &str) -> bool {
    matches!(
        name,
        "seq"
            | "op"
            | "deleted"
            | "schema_version"
            | "_seq"
            | "_op"
            | "_deleted"
            | "_schema_version"
    )
}

pub(super) fn clean_rows_from_batch(
    batch: &RecordBatch,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    application_columns: &[String],
    pk_filter: Option<&PkValues>,
) -> Result<Vec<CleanColdRow>, String> {
    let seq = required_column(batch, ColdMetadataColumn::Seq.name())?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "cold seq column has unexpected Arrow type".to_string())?;
    let op = required_column(batch, ColdMetadataColumn::Op.name())?
        .as_any()
        .downcast_ref::<Int16Array>()
        .ok_or_else(|| "cold op column has unexpected Arrow type".to_string())?;
    let deleted = required_column(batch, ColdMetadataColumn::Deleted.name())?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "cold deleted column has unexpected Arrow type".to_string())?;
    let schema_version = required_column(batch, ColdMetadataColumn::SchemaVersion.name())?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| "cold schema_version column has unexpected Arrow type".to_string())?;

    let decode_columns: Vec<&PgColumn> = columns
        .iter()
        .filter(|column| application_columns.iter().any(|name| name == &column.name))
        .collect();

    let pk_array = pk_filter
        .map(|pk| required_column(batch, &pk.column))
        .transpose()?;

    let mut rows = Vec::new();
    for row_index in 0..batch.num_rows() {
        // Exact PK equality before JSON materialization — point lookups would
        // otherwise encode every row in the selected row group (~1k rows).
        if let (Some(pk), Some(array)) = (pk_filter, pk_array) {
            if !arrow_cell_matches_pk_values(array, row_index, &pk.values) {
                continue;
            }
        }
        let deleted_value = deleted.value(row_index);
        let mut row_image = RowImage::with_capacity(decode_columns.len());
        for column in &decode_columns {
            let value = match batch.column_by_name(&column.name) {
                Some(array) => crate::pg_type_codec::cell_from_arrow_cell(
                    column.pg_type,
                    &column.name,
                    array.as_ref(),
                    row_index,
                )?,
                None if primary_key_columns.iter().any(|pk| pk == &column.name) => {
                    return Err(format!(
                        "cold segment is missing required primary-key column `{}`",
                        column.name
                    ));
                }
                None => koldstore_common::CellValue::Null,
            };
            row_image.insert(column.name.clone(), value);
        }
        let mut pk_json = serde_json::Map::new();
        for column in primary_key_columns {
            let cell = row_image.get(column).ok_or_else(|| {
                format!("cold row is missing primary-key field `{column}`")
            })?;
            pk_json.insert(column.clone(), cell.to_json());
        }
        if deleted_value {
            // Delete markers keep PK identity only; drop application payload.
            row_image = RowImage::new();
        }
        let seq_value = seq.value(row_index);
        let op_value = op.value(row_index);
        if !(1..=3).contains(&op_value) {
            return Err(format!(
                "cold segment has invalid op {op_value} at seq {seq_value}"
            ));
        }
        rows.push(CleanColdRow {
            pk_json: serde_json::Value::Object(pk_json),
            row_image,
            seq: seq_value,
            op: op_value,
            deleted: deleted_value,
            schema_version: schema_version.value(row_index),
        });
    }
    Ok(rows)
}

fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array, String> {
    batch
        .column_by_name(name)
        .map(|column| column.as_ref())
        .ok_or_else(|| format!("cold segment is missing required column `{name}`"))
}
