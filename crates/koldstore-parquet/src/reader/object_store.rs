//! ObjectStore-backed Parquet cold reads (footer-first, range GETs).

use std::sync::Arc;

use futures_util::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

use crate::object_reader::ObjectStoreParquetReader;
use crate::prune::{
    bloom_may_contain, column_index, prune_row_groups_by_seq_stats, select_row_groups_from_metadata,
};
use crate::schema::PgColumn;

use super::decode::{application_columns_for_read, clean_rows_from_batch, projection_mask};
use super::options::{BloomPruneMode, ParquetReadOptions, ParquetReadProfile, PkValues};
use super::types::CleanColdRow;

/// Reads clean-schema cold rows via ObjectStore range requests.
///
/// Only the Parquet footer is fetched eagerly. Row-group min/max pruning uses
/// footer stats; bloom filters are range-fetched only when multiple row groups
/// still overlap. Column chunks for selected row groups are fetched on demand.
///
/// Sync wrapper for PostgreSQL SPI / custom-scan callers.
///
/// # Errors
///
/// Returns an error when the object cannot be opened, Parquet decoding fails,
/// projection is invalid, or required metadata/primary-key columns are missing.
pub fn read_clean_cold_rows_from_object_store(
    store: Arc<dyn ::object_store::ObjectStore>,
    object_path: &str,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<Vec<CleanColdRow>, String> {
    Ok(read_clean_cold_rows_from_object_store_with_size(
        store,
        object_path,
        None,
        columns,
        primary_key_columns,
        options,
    )?
    .0)
}

/// Like [`read_clean_cold_rows_from_object_store`], with an optional known
/// object size so footer metadata uses a bounded range GET instead of a suffix
/// request (important for S3 backends that do not support suffix ranges).
///
/// Returns `(rows, profile)` so callers can surface footer/bloom/I/O details in
/// EXPLAIN and tracing.
///
/// # Errors
///
/// Returns an error when the object cannot be opened, Parquet decoding fails,
/// projection is invalid, or required metadata/primary-key columns are missing.
pub fn read_clean_cold_rows_from_object_store_with_size(
    store: Arc<dyn ::object_store::ObjectStore>,
    object_path: &str,
    file_size: Option<u64>,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<(Vec<CleanColdRow>, ParquetReadProfile), String> {
    let io = Arc::new(crate::object_reader::ObjectStoreReadStats::default());
    read_clean_cold_rows_from_object_store_with_stats(
        store,
        object_path,
        file_size,
        Some(io),
        columns,
        primary_key_columns,
        options,
    )
}

/// Like [`read_clean_cold_rows_from_object_store_with_size`], with optional I/O
/// counters for tests proving range-only ObjectStore access.
///
/// # Errors
///
/// Returns an error when ObjectStore I/O or Parquet decoding fails.
pub fn read_clean_cold_rows_from_object_store_with_stats(
    store: Arc<dyn ::object_store::ObjectStore>,
    object_path: &str,
    file_size: Option<u64>,
    stats: Option<Arc<crate::object_reader::ObjectStoreReadStats>>,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<(Vec<CleanColdRow>, ParquetReadProfile), String> {
    match koldstore_storage::runtime::block_on(
        read_clean_cold_rows_from_object_store_async(
            store,
            object_path,
            file_size,
            stats,
            columns,
            primary_key_columns,
            options,
        ),
        options.timeout,
    ) {
        Ok(result) => result,
        Err(_elapsed) => {
            let timeout_ms = options
                .timeout
                .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0);
            Err(format!(
                "object store parquet read timed out after {timeout_ms}ms"
            ))
        }
    }
}

/// Async ObjectStore-backed cold read (footer-first, range GETs).
///
/// # Errors
///
/// Returns an error when ObjectStore I/O or Parquet decoding fails.
pub async fn read_clean_cold_rows_from_object_store_async(
    store: Arc<dyn ::object_store::ObjectStore>,
    object_path: &str,
    file_size: Option<u64>,
    stats: Option<Arc<crate::object_reader::ObjectStoreReadStats>>,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<(Vec<CleanColdRow>, ParquetReadProfile), String> {
    let io =
        stats.unwrap_or_else(|| Arc::new(crate::object_reader::ObjectStoreReadStats::default()));
    let footer_cache_hit = crate::footer_cache::get(object_path, file_size).is_some();
    let mut reader = ObjectStoreParquetReader::from_key(store, object_path)?;
    if let Some(size) = file_size {
        reader = reader.with_file_size(size);
    }
    reader = reader.with_stats(Arc::clone(&io));
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|error| error.to_string())?;

    let application_columns = application_columns_for_read(columns, primary_key_columns, options)?;

    let total_row_groups = builder.metadata().num_row_groups();
    let mut selected_row_groups = options
        .row_groups
        .clone()
        .unwrap_or_else(|| (0..total_row_groups).collect());
    let mut pruning_applied = options.row_groups.is_some();
    let mut stats_pruned = false;
    let mut bloom_mode = BloomPruneMode::NotRequested;
    let mut bloom_filters_fetched = 0usize;

    // Seq-range prune from footer stats (no extra I/O) — same as kalamdb.
    if let Some(seq_range) = &options.seq_range {
        let before = selected_row_groups.len();
        selected_row_groups = prune_row_groups_by_seq_stats(
            builder.metadata(),
            builder.parquet_schema(),
            &selected_row_groups,
            &seq_range.column,
            seq_range.min.get(),
            seq_range.max.get(),
        );
        stats_pruned |= selected_row_groups.len() < before;
        pruning_applied = true;
    }

    if let Some(pk) = &options.pk_values {
        bloom_mode = BloomPruneMode::SkippedAfterStats;
        let (stats_selected, _) = select_row_groups_from_metadata(
            builder.metadata(),
            builder.parquet_schema(),
            &pk.column,
            &pk.values,
        )?;
        // Catalog-selected row groups are a prefilter. Footer statistics refine
        // conservative groups whose packed catalog bounds were unavailable.
        let stats_selected: Vec<usize> = if pruning_applied {
            stats_selected
                .into_iter()
                .filter(|idx| selected_row_groups.contains(idx))
                .collect()
        } else {
            stats_selected
        };
        stats_pruned |= stats_selected.len() < selected_row_groups.len();
        if stats_selected.is_empty() {
            let (range_calls, bytes_read) = io.snapshot();
            return Ok((
                Vec::new(),
                ParquetReadProfile {
                    object_path: object_path.to_string(),
                    file_size,
                    footer_first: true,
                    row_groups_total: total_row_groups,
                    row_groups_selected: Vec::new(),
                    row_groups_skipped: total_row_groups,
                    stats_pruned: true,
                    bloom: bloom_mode,
                    bloom_filters_fetched: 0,
                    projected_columns: application_columns,
                    pk_probe: Some((pk.column.clone(), pk.values.clone())),
                    range_calls,
                    bytes_read,
                    rows_returned: 0,
                    footer_cache_hit,
                },
            ));
        }
        selected_row_groups = if stats_selected.len() <= 1 {
            // Point lookups on seq-ordered flush segments usually collapse
            // here — skip bloom range GETs entirely.
            bloom_mode = BloomPruneMode::SkippedAfterStats;
            stats_selected
        } else {
            let (refined, fetched) =
                refine_row_groups_with_bloom(&mut builder, &stats_selected, pk).await?;
            bloom_mode = BloomPruneMode::Applied;
            bloom_filters_fetched = fetched;
            refined
        };
        pruning_applied = true;
    }

    if pruning_applied {
        if selected_row_groups.is_empty() {
            let (range_calls, bytes_read) = io.snapshot();
            return Ok((
                Vec::new(),
                ParquetReadProfile {
                    object_path: object_path.to_string(),
                    file_size,
                    footer_first: true,
                    row_groups_total: total_row_groups,
                    row_groups_selected: Vec::new(),
                    row_groups_skipped: total_row_groups,
                    stats_pruned,
                    bloom: bloom_mode,
                    bloom_filters_fetched,
                    projected_columns: application_columns,
                    pk_probe: options
                        .pk_values
                        .as_ref()
                        .map(|pk| (pk.column.clone(), pk.values.clone())),
                    range_calls,
                    bytes_read,
                    rows_returned: 0,
                    footer_cache_hit,
                },
            ));
        }
        builder = builder.with_row_groups(selected_row_groups.clone());
    }

    if !options.columns.is_empty() {
        let mask = projection_mask(builder.parquet_schema(), &application_columns);
        builder = builder.with_projection(mask);
    }

    let mut stream = builder.build().map_err(|error| error.to_string())?;
    let pk_filter = options.pk_values.as_ref();
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| error.to_string())?;
        rows.extend(clean_rows_from_batch(
            &batch,
            columns,
            primary_key_columns,
            &application_columns,
            pk_filter,
        )?);
    }

    let selected = if pruning_applied {
        selected_row_groups
    } else {
        (0..total_row_groups).collect()
    };
    let (range_calls, bytes_read) = io.snapshot();
    let profile = ParquetReadProfile {
        object_path: object_path.to_string(),
        file_size,
        footer_first: true,
        row_groups_total: total_row_groups,
        row_groups_skipped: total_row_groups.saturating_sub(selected.len()),
        row_groups_selected: selected,
        stats_pruned,
        bloom: bloom_mode,
        bloom_filters_fetched,
        projected_columns: application_columns,
        pk_probe: options
            .pk_values
            .as_ref()
            .map(|pk| (pk.column.clone(), pk.values.clone())),
        range_calls,
        bytes_read,
        rows_returned: rows.len(),
        footer_cache_hit,
    };
    Ok((rows, profile))
}

async fn refine_row_groups_with_bloom(
    builder: &mut ParquetRecordBatchStreamBuilder<ObjectStoreParquetReader>,
    candidates: &[usize],
    pk: &PkValues,
) -> Result<(Vec<usize>, usize), String> {
    let column_idx = column_index(builder.parquet_schema(), &pk.column)?;
    let physical_type = builder.parquet_schema().column(column_idx).physical_type();
    let mut selected = Vec::with_capacity(candidates.len());
    let mut fetched = 0usize;
    for &rg_index in candidates {
        match builder
            .get_row_group_column_bloom_filter(rg_index, column_idx)
            .await
        {
            Ok(Some(bloom)) => {
                fetched += 1;
                if pk
                    .values
                    .iter()
                    .any(|value| bloom_may_contain(&bloom, physical_type, value))
                {
                    selected.push(rg_index);
                }
            }
            Ok(None) | Err(_) => selected.push(rg_index),
        }
    }
    Ok((selected, fetched))
}
