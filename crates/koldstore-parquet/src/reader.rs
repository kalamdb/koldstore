//! Direct ObjectStore-backed Parquet reader surface.
//!
//! Cold reads prefer [`read_clean_cold_rows_from_object_store`]: footer metadata
//! is loaded first via suffix/range GET, row groups are pruned (min/max + bloom),
//! then only selected column chunks are fetched. Local-path and in-memory helpers
//! remain for tests and flush validation.

use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, UInt32Array};
use bytes::Bytes;
use futures_util::StreamExt;
use koldstore_common::{ColdRow, CommitSeq, LogicalPk, PkColumn, SeqId};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::reader::ChunkReader;
use parquet::schema::types::SchemaDescriptor;

use crate::object_reader::ObjectStoreParquetReader;
use crate::prune::{bloom_may_contain, column_index, select_row_groups_from_metadata};
use crate::schema::{ColdMetadataColumn, PgColumn};

/// Boxed record-batch stream.
pub type RecordBatchFileStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<RecordBatch, String>> + Send>>;

/// Read options for projection and pruning.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetReadOptions {
    pub columns: Vec<String>,
    pub row_groups: Option<Vec<usize>>,
    pub seq_range: Option<SeqRange>,
    pub commit_seq_range: Option<CommitSeqRange>,
    pub pk_values: Option<PkValues>,
}

impl ParquetReadOptions {
    /// Creates default read options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds projection columns.
    #[must_use]
    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = columns.into_iter().map(Into::into).collect();
        self
    }

    /// Projects clean-schema change metadata columns.
    #[must_use]
    pub fn with_clean_change_metadata(mut self) -> Self {
        self.columns = vec![
            "seq".to_string(),
            "op".to_string(),
            "deleted".to_string(),
            "schema_version".to_string(),
        ];
        self
    }

    /// Adds selected row groups after footer/stat/bloom pruning.
    #[must_use]
    pub fn with_row_groups<I>(mut self, row_groups: I) -> Self
    where
        I: IntoIterator<Item = usize>,
    {
        self.row_groups = Some(row_groups.into_iter().collect());
        self
    }

    /// Adds clean-schema `seq` range pruning.
    #[must_use]
    pub fn with_clean_seq_range(mut self, min: SeqId, max: SeqId) -> Self {
        self.seq_range = Some(SeqRange {
            column: crate::schema::ColdMetadataColumn::Seq.name().to_string(),
            min,
            max,
        });
        self
    }

    /// Adds sequence range pruning for the given column name.
    #[must_use]
    pub fn with_seq_range(mut self, column: impl Into<String>, min: SeqId, max: SeqId) -> Self {
        self.seq_range = Some(SeqRange {
            column: column.into(),
            min,
            max,
        });
        self
    }

    /// Adds commit-sequence range pruning for the given column name.
    #[must_use]
    pub fn with_commit_seq_range(
        mut self,
        column: impl Into<String>,
        min: CommitSeq,
        max: CommitSeq,
    ) -> Self {
        self.commit_seq_range = Some(CommitSeqRange {
            column: column.into(),
            min,
            max,
        });
        self
    }

    /// Adds PK may-contain values for bloom/exact pruning.
    #[must_use]
    pub fn with_pk_values<I, S>(mut self, column: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.pk_values = Some(PkValues {
            column: column.into(),
            values: values.into_iter().map(Into::into).collect(),
        });
        self
    }
}

/// Sequence range pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqRange {
    pub column: String,
    pub min: SeqId,
    pub max: SeqId,
}

/// Commit sequence range pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSeqRange {
    pub column: String,
    pub min: CommitSeq,
    pub max: CommitSeq,
}

/// PK values for pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkValues {
    pub column: String,
    pub values: Vec<String>,
}

/// How PK bloom filters were used during a Parquet read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BloomPruneMode {
    /// No PK equality probe was requested.
    #[default]
    NotRequested,
    /// Min/max already left ≤1 row group; bloom pages were not fetched.
    SkippedAfterStats,
    /// Bloom pages were range-fetched to refine overlapping row groups.
    Applied,
}

/// Per-segment ObjectStore Parquet read diagnostics for EXPLAIN / tracing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParquetReadProfile {
    /// Object key that was read.
    pub object_path: String,
    /// Known object size when provided by the catalog (bounded footer GET).
    pub file_size: Option<u64>,
    /// Footer was loaded via ObjectStore range/suffix GET before column data.
    pub footer_first: bool,
    /// Total row groups in the file footer.
    pub row_groups_total: usize,
    /// Row groups kept after min/max (+ optional bloom) pruning.
    pub row_groups_selected: Vec<usize>,
    /// Row groups skipped by pruning.
    pub row_groups_skipped: usize,
    /// Whether footer column-chunk min/max stats pruned any row groups.
    pub stats_pruned: bool,
    /// Bloom filter usage for this read.
    pub bloom: BloomPruneMode,
    /// Number of bloom filters actually range-fetched.
    pub bloom_filters_fetched: usize,
    /// Projected application column names (plus required cold metadata).
    pub projected_columns: Vec<String>,
    /// PK equality probe values when present.
    pub pk_probe: Option<(String, Vec<String>)>,
    /// ObjectStore range GET call count (footer + bloom + column chunks).
    pub range_calls: u64,
    /// Total bytes returned by those range GETs.
    pub bytes_read: u64,
    /// Decoded clean cold rows after exact PK filter.
    pub rows_returned: usize,
}

impl BloomPruneMode {
    /// Short label for EXPLAIN.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotRequested => "not_requested",
            Self::SkippedAfterStats => "skipped_after_stats",
            Self::Applied => "applied",
        }
    }
}

impl ParquetReadProfile {
    /// Compact I/O summary for EXPLAIN / tracing.
    #[must_use]
    pub fn format_io_summary(&self) -> String {
        let mut parts = Vec::new();
        if self.footer_first {
            parts.push("footer-first".to_string());
        }
        parts.push(format!(
            "range_gets={}, bytes_read={}",
            self.range_calls, self.bytes_read
        ));
        if let Some(size) = self.file_size {
            if size > 0 && self.bytes_read < size {
                let pct = (self.bytes_read as f64 * 100.0) / size as f64;
                parts.push(format!("{pct:.1}% of object"));
            }
        }
        parts.join(", ")
    }

    /// Compact row-group prune summary for EXPLAIN / tracing.
    #[must_use]
    pub fn format_row_groups_summary(&self) -> String {
        let selected = if self.row_groups_selected.is_empty() {
            "none".to_string()
        } else {
            self.row_groups_selected
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        };
        format!(
            "total={}, selected=[{}], skipped={}, stats_pruned={}",
            self.row_groups_total, selected, self.row_groups_skipped, self.stats_pruned
        )
    }

    /// Compact bloom summary for EXPLAIN / tracing.
    #[must_use]
    pub fn format_bloom_summary(&self) -> String {
        match self.bloom {
            BloomPruneMode::NotRequested => "not_requested".to_string(),
            BloomPruneMode::SkippedAfterStats => {
                "skipped_after_stats (min/max left ≤1 row group)".to_string()
            }
            BloomPruneMode::Applied => {
                format!("applied, filters_fetched={}", self.bloom_filters_fetched)
            }
        }
    }
}

/// Direct object-store Parquet read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetReadRequest {
    /// Final object-store path.
    pub object_path: String,
    /// Projection and pruning options.
    pub options: ParquetReadOptions,
}

/// Logical row read from a clean-schema cold Parquet segment.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CleanColdRow {
    /// Primary-key values encoded by column name.
    pub pk_json: serde_json::Value,
    /// Base table row image, or `null` for delete markers.
    pub row_image: serde_json::Value,
    /// KoldStore sequence number.
    pub seq: i64,
    /// Commit sequence used for winner ordering. Clean segments currently use
    /// `seq` as the commit ordering value.
    pub commit_seq: i64,
    /// Whether this row is a cold delete marker.
    pub deleted: bool,
    /// Schema version used to write the segment.
    pub schema_version: u32,
}

impl ParquetReadRequest {
    /// Creates a direct Parquet read request.
    #[must_use]
    pub fn new(object_path: impl Into<String>, options: ParquetReadOptions) -> Self {
        Self {
            object_path: object_path.into(),
            options,
        }
    }

    /// Returns true because the direct reader inspects footer metadata before column chunks.
    #[must_use]
    pub const fn uses_footer_before_columns(&self) -> bool {
        true
    }

    /// Returns true when PK bloom/may-contain metadata can be checked.
    #[must_use]
    pub fn uses_pk_bloom_checks(&self) -> bool {
        self.options.pk_values.is_some()
    }
}

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
    store: Arc<dyn ObjectStore>,
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
    store: Arc<dyn ObjectStore>,
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
    store: Arc<dyn ObjectStore>,
    object_path: &str,
    file_size: Option<u64>,
    stats: Option<Arc<crate::object_reader::ObjectStoreReadStats>>,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<(Vec<CleanColdRow>, ParquetReadProfile), String> {
    block_on(read_clean_cold_rows_from_object_store_async(
        store,
        object_path,
        file_size,
        stats,
        columns,
        primary_key_columns,
        options,
    ))
}

/// Async ObjectStore-backed cold read (footer-first, range GETs).
///
/// # Errors
///
/// Returns an error when ObjectStore I/O or Parquet decoding fails.
pub async fn read_clean_cold_rows_from_object_store_async(
    store: Arc<dyn ObjectStore>,
    object_path: &str,
    file_size: Option<u64>,
    stats: Option<Arc<crate::object_reader::ObjectStoreReadStats>>,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<(Vec<CleanColdRow>, ParquetReadProfile), String> {
    let io =
        stats.unwrap_or_else(|| Arc::new(crate::object_reader::ObjectStoreReadStats::default()));
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
    if let Some(commit_range) = &options.commit_seq_range {
        let before = selected_row_groups.len();
        selected_row_groups = prune_row_groups_by_seq_stats(
            builder.metadata(),
            builder.parquet_schema(),
            &selected_row_groups,
            &commit_range.column,
            commit_range.min.get(),
            commit_range.max.get(),
        );
        stats_pruned |= selected_row_groups.len() < before;
        pruning_applied = true;
    }

    if options.row_groups.is_none() {
        if let Some(pk) = &options.pk_values {
            bloom_mode = BloomPruneMode::SkippedAfterStats;
            let (stats_selected, _) = select_row_groups_from_metadata(
                builder.metadata(),
                builder.parquet_schema(),
                &pk.column,
                &pk.values,
            )?;
            // Intersect with any prior seq prune.
            let stats_selected: Vec<usize> = if pruning_applied {
                stats_selected
                    .into_iter()
                    .filter(|idx| selected_row_groups.contains(idx))
                    .collect()
            } else {
                stats_selected
            };
            stats_pruned |= stats_selected.len() < total_row_groups;
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
    };
    Ok((rows, profile))
}

/// Footer-stats seq/commit-seq prune (no I/O).
fn prune_row_groups_by_seq_stats(
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema: &SchemaDescriptor,
    row_groups: &[usize],
    column: &str,
    min: i64,
    max: i64,
) -> Vec<usize> {
    let Some(column_idx) = schema.columns().iter().position(|c| c.name() == column) else {
        return row_groups.to_vec();
    };
    row_groups
        .iter()
        .copied()
        .filter(|&rg_index| {
            let Some(stats) = metadata.row_group(rg_index).column(column_idx).statistics() else {
                return true;
            };
            match stats {
                parquet::file::statistics::Statistics::Int64(values) => values
                    .min_opt()
                    .zip(values.max_opt())
                    .map(|(group_min, group_max)| *group_max >= min && *group_min <= max)
                    .unwrap_or(true),
                parquet::file::statistics::Statistics::Int32(values) => values
                    .min_opt()
                    .zip(values.max_opt())
                    .map(|(group_min, group_max)| {
                        i64::from(*group_max) >= min && i64::from(*group_min) <= max
                    })
                    .unwrap_or(true),
                _ => true,
            }
        })
        .collect()
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

/// Reads clean-schema cold rows from a local Parquet file.
///
/// # Errors
///
/// Returns an error when the file cannot be opened, Parquet decoding fails, or
/// required metadata/primary-key columns are missing.
pub fn read_clean_cold_rows_from_path(
    path: impl AsRef<Path>,
    columns: &[PgColumn],
    primary_key_columns: &[String],
) -> Result<Vec<CleanColdRow>, String> {
    read_clean_cold_rows_with_options(
        path,
        columns,
        primary_key_columns,
        &ParquetReadOptions::default(),
    )
}

/// Reads clean-schema cold rows from a local Parquet file with projection and row-group options.
///
/// When `options.columns` is non-empty, only those application columns are decoded in addition
/// to required cold metadata (`seq`, `deleted`, `schema_version`). Every primary-key column must
/// appear in the projection or this function returns an error.
///
/// When `options.row_groups` is set, only the selected row groups are scanned.
/// When `options.pk_values` is set and `row_groups` is unset, row groups are pruned first via
/// column-chunk min/max and native Parquet bloom filters on a single file handle.
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

/// Reads clean-schema cold rows from an in-memory Parquet object.
///
/// # Errors
///
/// Returns an error when Parquet decoding, projection, or PK pruning fails.
pub fn read_clean_cold_rows_from_bytes(
    bytes: Bytes,
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<Vec<CleanColdRow>, String> {
    read_clean_cold_rows_from_reader(bytes, columns, primary_key_columns, options)
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
    if effective.row_groups.is_none() {
        if let Some(pk) = &options.pk_values {
            let (mut selected, _) = select_row_groups_from_metadata(
                builder.metadata(),
                builder.parquet_schema(),
                &pk.column,
                &pk.values,
            )?;
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
        commit_seq: CommitSeq::new(row.commit_seq).map_err(|error| error.to_string())?,
        deleted: row.deleted,
        schema_version: row.schema_version,
        row_image: row.row_image,
    })
}

fn application_columns_for_read(
    columns: &[PgColumn],
    primary_key_columns: &[String],
    options: &ParquetReadOptions,
) -> Result<Vec<String>, String> {
    if options.columns.is_empty() {
        return Ok(columns.iter().map(|column| column.name.clone()).collect());
    }
    for pk in primary_key_columns {
        if !options.columns.iter().any(|column| column == pk) {
            return Err(format!(
                "parquet read projection is missing required primary-key column `{pk}`"
            ));
        }
    }
    Ok(options.columns.clone())
}

fn projection_mask(schema: &SchemaDescriptor, application_columns: &[String]) -> ProjectionMask {
    let mut names = vec![
        ColdMetadataColumn::Seq.name(),
        ColdMetadataColumn::Deleted.name(),
        ColdMetadataColumn::SchemaVersion.name(),
    ];
    for column in application_columns {
        if !names.iter().any(|name| name == column) {
            names.push(column.as_str());
        }
    }
    ProjectionMask::columns(schema, names)
}

fn clean_rows_from_batch(
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
        let mut row_image = serde_json::Map::new();
        for column in &decode_columns {
            let value = match batch.column_by_name(&column.name) {
                Some(array) => crate::pg_type_codec::json_from_arrow_cell(
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
                None => serde_json::Value::Null,
            };
            row_image.insert(column.name.clone(), value);
        }
        let mut pk_json = serde_json::Map::new();
        for column in primary_key_columns {
            pk_json.insert(
                column.clone(),
                row_image
                    .get(column)
                    .cloned()
                    .ok_or_else(|| format!("cold row is missing primary-key field `{column}`"))?,
            );
        }
        let row_image = if deleted_value {
            serde_json::Value::Null
        } else {
            serde_json::Value::Object(row_image)
        };
        let seq_value = seq.value(row_index);
        rows.push(CleanColdRow {
            pk_json: serde_json::Value::Object(pk_json),
            row_image,
            seq: seq_value,
            commit_seq: seq_value,
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

fn parquet_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("koldstore-parquet")
            .enable_all()
            .build()
            .expect("create tokio runtime for parquet object-store IO")
    })
}

fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| parquet_runtime().block_on(future))
    } else {
        parquet_runtime().block_on(future)
    }
}
