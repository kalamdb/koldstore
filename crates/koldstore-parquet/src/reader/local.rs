//! Local-path and in-memory Parquet cold reads.

use std::path::Path;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;

use crate::prune::{bloom_may_contain, column_index, select_row_groups_from_metadata};
use crate::schema::PgColumn;

use super::decode::{application_columns_for_read, clean_rows_from_batch, projection_mask};
use super::options::ParquetReadOptions;
use super::types::CleanColdRow;

/// Reads clean-schema cold rows from a local Parquet file with projection and row-group options.
///
/// When `options.columns` is non-empty, only those application columns are decoded in addition
/// to required cold metadata (`seq`, `deleted`, `schema_version`). Every primary-key column must
/// appear in the projection or this function returns an error.
///
/// When `options.row_groups` is set, only the selected row groups are scanned.
/// When `options.pk_values` is set, footer min/max and native Parquet bloom
/// filters refine any catalog-selected row groups on the same file handle.
///
/// # Errors
///
/// Returns an error when the file cannot be opened, Parquet decoding fails, projection is
/// invalid, or required metadata/primary-key columns are missing.
pub fn read_clean_cold_rows_with_options(
    path: impl AsRef<Path>,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<Vec<CleanColdRow>, String> {
    let file = std::fs::File::open(path.as_ref()).map_err(|error| error.to_string())?;
    read_clean_cold_rows_from_reader(file, columns, primary_key_columns, options)
}

fn read_clean_cold_rows_from_reader<R>(
    reader: R,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<Vec<CleanColdRow>, String>
where
    R: ChunkReader + 'static,
{
    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|error| error.to_string())?;
    let application_columns = application_columns_for_read(columns, primary_key_columns, options)?;

    let mut effective = options.clone();
    if let Some(pk) = &options.pk_values {
        let (selected_from_footer, _) = select_row_groups_from_metadata(
            builder.metadata(),
            builder.parquet_schema(),
            &pk.column,
            &pk.values,
        )?;
        let mut selected = if let Some(catalog_selected) = &effective.row_groups {
            selected_from_footer
                .into_iter()
                .filter(|row_group| catalog_selected.contains(row_group))
                .collect()
        } else {
            selected_from_footer
        };
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        if selected.len() > 1 {
            let column_idx = column_index(builder.parquet_schema(), &pk.column)?;
            let physical_type = builder.parquet_schema().column(column_idx).physical_type();
            let mut refined = Vec::new();
            for rg_index in selected {
                match builder.get_row_group_column_bloom_filter(rg_index, column_idx) {
                    Ok(Some(bloom)) => {
                        if pk
                            .values
                            .iter()
                            .any(|value| bloom_may_contain(&bloom, physical_type, value))
                        {
                            refined.push(rg_index);
                        }
                    }
                    Ok(None) | Err(_) => refined.push(rg_index),
                }
            }
            selected = refined;
        }
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        effective.row_groups = Some(selected);
    }

    if !effective.columns.is_empty() {
        let mask = projection_mask(builder.parquet_schema(), &application_columns);
        builder = builder.with_projection(mask);
    }
    if let Some(row_groups) = &effective.row_groups {
        builder = builder.with_row_groups(row_groups.clone());
    }
    let reader = builder.build().map_err(|error| error.to_string())?;
    let pk_filter = effective.pk_values.as_ref();
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| error.to_string())?;
        rows.extend(clean_rows_from_batch(
            &batch,
            columns,
            primary_key_columns,
            &application_columns,
            pk_filter,
        )?);
    }
    Ok(rows)
}
