//! ObjectStore-backed Parquet cold reads (footer-first, range GETs).

use std::sync::Arc;
use std::time::Instant;

use futures_util::StreamExt;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::PageIndexPolicy;

use crate::object_reader::ObjectStoreParquetReader;
use crate::page_prune::{row_selection_for_equality_values, PagePruneDecision};
use crate::prune::{
    bloom_may_contain, column_index, prune_row_groups_by_seq_stats, select_row_groups_from_metadata,
};
use crate::schema::PgColumn;

use super::decode::{application_columns_for_read, clean_rows_from_batch, projection_mask};
use super::options::{
    BloomPruneMode, ParquetProfileMode, ParquetReadOptions, ParquetReadProfile, PkValues,
};
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
/// EXPLAIN and tracing. The profile is empty unless `options.profile_mode`
/// explicitly enables diagnostic collection.
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
    let io = read_stats_for(options.profile_mode);
    read_clean_cold_rows_from_object_store_with_stats(
        store,
        object_path,
        file_size,
        io,
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
    let collect_profile = options.profile_mode.collects_counts();
    let collect_timing = options.profile_mode.collects_timing();
    let io = stats.or_else(|| read_stats_for(options.profile_mode));
    let open_started = collect_timing.then(Instant::now);
    let footer_cache_hit =
        collect_profile && crate::footer_cache::get(object_path, file_size).is_some();
    let mut reader = ObjectStoreParquetReader::from_key(store, object_path)?;
    if let Some(size) = file_size {
        reader = reader.with_file_size(size);
    }
    if let Some(io) = &io {
        reader = reader.with_stats(Arc::clone(io));
    }
    // Load page indexes only for equality probes; other paths keep the lighter
    // Skip footer (and remain eligible for the footer cache).
    let reader_options = if options.pk_values.is_some() {
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional)
    } else {
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip)
    };
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, reader_options)
        .await
        .map_err(|error| error.to_string())?;
    let open_duration = open_started
        .map(|started| started.elapsed())
        .unwrap_or_default();
    let scan_started = collect_timing.then(Instant::now);

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
    let mut page_prune = PagePruneDecision::not_requested();

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
            if !collect_profile {
                return Ok((Vec::new(), ParquetReadProfile::default()));
            }
            let io_snapshot = io
                .as_ref()
                .map(|stats| stats.timed_snapshot())
                .unwrap_or_default();
            return Ok((
                Vec::new(),
                empty_read_profile(ParquetReadProfile {
                    object_path: object_path.to_string(),
                    file_size,
                    row_groups_total: total_row_groups,
                    stats_pruned: true,
                    bloom: bloom_mode,
                    projected_columns: application_columns,
                    pk_probe: Some((pk.column.clone(), pk.values.clone())),
                    range_calls: io_snapshot.range_calls,
                    bytes_read: io_snapshot.bytes_read,
                    footer_cache_hit,
                    open_duration,
                    scan_duration: scan_started
                        .map(|started| started.elapsed())
                        .unwrap_or_default(),
                    object_store_read_duration: io_snapshot.read_duration,
                    ..Default::default()
                }),
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

        page_prune = row_selection_for_equality_values(
            builder.metadata(),
            builder.parquet_schema(),
            &selected_row_groups,
            &pk.column,
            &pk.values,
        )?;
    }

    if pruning_applied {
        if selected_row_groups.is_empty() {
            if !collect_profile {
                return Ok((Vec::new(), ParquetReadProfile::default()));
            }
            let io_snapshot = io
                .as_ref()
                .map(|stats| stats.timed_snapshot())
                .unwrap_or_default();
            return Ok((
                Vec::new(),
                empty_read_profile(ParquetReadProfile {
                    object_path: object_path.to_string(),
                    file_size,
                    row_groups_total: total_row_groups,
                    stats_pruned,
                    bloom: bloom_mode,
                    bloom_filters_fetched,
                    page_index: page_prune.mode,
                    pages_total: page_prune.pages_total,
                    pages_selected: page_prune.pages_selected,
                    pages_skipped: page_prune.pages_skipped,
                    projected_columns: application_columns,
                    pk_probe: options
                        .pk_values
                        .as_ref()
                        .map(|pk| (pk.column.clone(), pk.values.clone())),
                    range_calls: io_snapshot.range_calls,
                    bytes_read: io_snapshot.bytes_read,
                    footer_cache_hit,
                    open_duration,
                    scan_duration: scan_started
                        .map(|started| started.elapsed())
                        .unwrap_or_default(),
                    object_store_read_duration: io_snapshot.read_duration,
                    ..Default::default()
                }),
            ));
        }
        builder = builder.with_row_groups(selected_row_groups.clone());
    }

    if let Some(selection) = page_prune.selection.clone() {
        builder = builder.with_row_selection(selection);
    }

    if !options.columns.is_empty() {
        let mask = projection_mask(builder.parquet_schema(), &application_columns);
        builder = builder.with_projection(mask);
    }

    let mut stream = builder.build().map_err(|error| error.to_string())?;
    let pk_filter = options.pk_values.as_ref();
    let row_limit = options.row_limit;
    let seq_min = options.seq_range.as_ref().map(|range| range.min.get());
    let seq_max = options.seq_range.as_ref().map(|range| range.max.get());
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| error.to_string())?;
        let decoded = clean_rows_from_batch(
            &batch,
            columns,
            primary_key_columns,
            &application_columns,
            pk_filter,
        )?;
        for row in decoded {
            if seq_min.is_some_and(|min| row.seq < min) || seq_max.is_some_and(|max| row.seq > max)
            {
                continue;
            }
            rows.push(row);
            if row_limit.is_some_and(|limit| rows.len() >= limit) {
                break;
            }
        }
        if row_limit.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
    }

    if !collect_profile {
        return Ok((rows, ParquetReadProfile::default()));
    }
    let selected = if pruning_applied {
        selected_row_groups
    } else {
        (0..total_row_groups).collect()
    };
    let io_snapshot = io
        .as_ref()
        .map(|stats| stats.timed_snapshot())
        .unwrap_or_default();
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
        page_index: page_prune.mode,
        pages_total: page_prune.pages_total,
        pages_selected: page_prune.pages_selected,
        pages_skipped: page_prune.pages_skipped,
        projected_columns: application_columns,
        pk_probe: options
            .pk_values
            .as_ref()
            .map(|pk| (pk.column.clone(), pk.values.clone())),
        range_calls: io_snapshot.range_calls,
        bytes_read: io_snapshot.bytes_read,
        rows_returned: rows.len(),
        footer_cache_hit,
        open_duration,
        scan_duration: scan_started
            .map(|started| started.elapsed())
            .unwrap_or_default(),
        object_store_read_duration: io_snapshot.read_duration,
    };
    Ok((rows, profile))
}

fn read_stats_for(
    mode: ParquetProfileMode,
) -> Option<Arc<crate::object_reader::ObjectStoreReadStats>> {
    mode.collects_counts().then(|| {
        Arc::new(if mode.collects_timing() {
            crate::object_reader::ObjectStoreReadStats::with_timing()
        } else {
            crate::object_reader::ObjectStoreReadStats::default()
        })
    })
}

/// Builds a zero-row profile after prune eliminated every row group.
fn empty_read_profile(base: ParquetReadProfile) -> ParquetReadProfile {
    let total = base.row_groups_total;
    ParquetReadProfile {
        footer_first: true,
        row_groups_selected: Vec::new(),
        row_groups_skipped: total,
        rows_returned: 0,
        ..base
    }
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
