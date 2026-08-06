//! Mirror-row streaming encoder: typed rows → Arrow chunks.
//!
//! SPI fetch stays in `pg_koldstore`; this module owns the PG-free encode loop.
//! Post-flush cleanup uses a seq-range DELETE (see `cleanup::plan_seq_range_cleanup`)
//! so this path no longer materializes per-row cleanup JSON.

use koldstore_common::{ColumnRef, QualifiedTableName, SqlStatement};
use koldstore_parquet::{
    extract_packed_segment_metadata, CleanColdRecordBatchBuilder, ColdMetadataColumn,
    ColdRecordBatch, FlushMirrorRow, PgColumn, SegmentSplitPolicy, SortingColumnSpec,
    StreamingParquetSegmentWriter, WriterOptions,
};

use crate::write::FlushWriteChunk;

/// Input for one streaming flush encode pass.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEncodeInput {
    /// Managed user table.
    pub table: QualifiedTableName,
    /// Mirror table for the managed table.
    pub mirror: QualifiedTableName,
    /// Primary-key column names.
    pub primary_key_columns: Vec<String>,
    /// Application column names in catalog order.
    pub base_column_names: Vec<String>,
    /// Parquet schema columns.
    pub parquet_columns: Vec<PgColumn>,
    /// Indexed columns tracked for segment stats.
    pub indexed_columns: Vec<ColumnRef>,
    /// Active cold schema version.
    pub schema_version: u32,
    /// Maximum selected mirror `seq`.
    pub max_seq: i64,
    /// Maximum rows per Parquet segment file.
    pub max_rows_per_file: usize,
    /// SPI page size for mirror fetches (≤ [`crate::FLUSH_MIRROR_FETCH_BATCH_SIZE`]).
    pub fetch_batch_size: i64,
    /// Optional compressed-byte target for each Parquet segment.
    pub target_file_size_bytes: Option<u64>,
    /// Parquet compression codec.
    pub compression: String,
    /// Rows encoded per streaming row group.
    pub row_group_size: usize,
    /// When set, mirror fetch is restricted to these operation codes.
    pub mirror_ops: Option<Vec<i16>>,
    /// Collects the bounded flush selection and sorts by mirror `order_key`, then PK.
    pub sort_by_order_key: bool,
    /// Application column name for [`Self::sort_by_order_key`] (Parquet sort metadata).
    pub order_key_column: Option<String>,
}

struct SegmentBuilder {
    options: WriterOptions,
    split_policy: SegmentSplitPolicy,
    writer: Option<StreamingParquetSegmentWriter>,
    row_count: usize,
    parquet_columns: Vec<PgColumn>,
    indexed_columns: Vec<ColumnRef>,
    primary_key_columns: Vec<String>,
}

impl SegmentBuilder {
    fn new(input: &StreamEncodeInput) -> Self {
        let mut sorting_columns = Vec::new();
        if input.sort_by_order_key {
            if let Some(order_key) = input.order_key_column.as_deref() {
                sorting_columns.push(SortingColumnSpec::ascending(order_key));
            }
            for pk in &input.primary_key_columns {
                if input
                    .order_key_column
                    .as_deref()
                    .is_none_or(|order_key| order_key != pk)
                {
                    sorting_columns.push(SortingColumnSpec::ascending(pk));
                }
            }
        }
        // Mirror fetch / encode always writes ascending `seq` (heap-like insert
        // order for insert-only tables). Declare it natively so readers can trust
        // Parquet row-group sort metadata instead of inventing a custom order.
        sorting_columns.push(SortingColumnSpec::ascending(ColdMetadataColumn::Seq.name()));

        let options = WriterOptions {
            compression: input.compression.clone(),
            row_group_size: input.row_group_size.max(1),
            ..WriterOptions::default()
        }
        .with_statistics_columns(
            [ColdMetadataColumn::Seq.name()]
                .into_iter()
                .chain(input.primary_key_columns.iter().map(String::as_str))
                .chain(
                    input
                        .indexed_columns
                        .iter()
                        .map(|column| column.name.as_str()),
                ),
        )
        .with_bloom_filter_columns(input.primary_key_columns.iter().map(String::as_str))
        .with_sorting_columns(sorting_columns);
        Self {
            options,
            split_policy: SegmentSplitPolicy::new(
                input.target_file_size_bytes,
                input.max_rows_per_file,
            ),
            writer: None,
            row_count: 0,
            parquet_columns: input.parquet_columns.clone(),
            indexed_columns: input.indexed_columns.clone(),
            primary_key_columns: input.primary_key_columns.clone(),
        }
    }

    fn remaining_rows(&self, max_rows_per_file: usize) -> usize {
        max_rows_per_file.max(1).saturating_sub(self.row_count)
    }

    fn push_batch(&mut self, batch: ColdRecordBatch) -> Result<bool, String> {
        let batch_row_count = batch.batch.num_rows();
        let writer = if let Some(writer) = self.writer.as_mut() {
            writer
        } else {
            self.writer.insert(
                StreamingParquetSegmentWriter::try_new(batch.batch.schema(), self.options.clone())
                    .map_err(|error| error.to_string())?,
            )
        };
        writer
            .write_batch(&batch.batch)
            .map_err(|error| error.to_string())?;
        self.row_count = self.row_count.saturating_add(batch_row_count);
        // PERFORMANCE: drop Arrow immediately after encode so uncompressed row
        // groups never sit beside the growing compressed Parquet buffer.
        drop(batch);
        Ok(self
            .split_policy
            .should_close(writer.current_bytes(), self.row_count))
    }

    fn finish_segment(&mut self) -> Result<Option<FlushWriteChunk>, String> {
        let Some(writer) = self.writer.take() else {
            return Ok(None);
        };
        let encoded = writer
            .finish_with_metadata()
            .map_err(|error| error.to_string())?;
        let packed_metadata = extract_packed_segment_metadata(
            encoded.metadata.as_ref(),
            &self.parquet_columns,
            &self.indexed_columns,
            &self.primary_key_columns,
        )?;
        let chunk = FlushWriteChunk::from_encoded(encoded, packed_metadata);
        self.row_count = 0;
        Ok(Some(chunk))
    }
}

/// Outcome of streaming mirror rows into Parquet segment chunks.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEncodeOutcome {
    /// Inclusive upper bound of flushed mirror `seq` values.
    pub max_seq: i64,
    /// Number of mirror rows streamed from the fetch callback.
    pub rows_written: usize,
}

struct ChunkBuilder {
    parquet_columns: Vec<PgColumn>,
    batch_builder: CleanColdRecordBatchBuilder,
}

impl ChunkBuilder {
    fn new(parquet_columns: &[PgColumn]) -> Result<Self, String> {
        Ok(Self {
            parquet_columns: parquet_columns.to_vec(),
            batch_builder: CleanColdRecordBatchBuilder::new(parquet_columns)?,
        })
    }

    fn push_row(
        &mut self,
        row: &FlushMirrorRow,
        primary_key_columns: &[String],
        schema_version: u32,
    ) -> Result<(), String> {
        self.batch_builder.push_typed_row(
            &row.values,
            primary_key_columns,
            row.seq,
            row.op,
            schema_version,
        )
    }

    fn len(&self) -> usize {
        self.batch_builder.row_count()
    }

    fn take_batch(&mut self) -> Result<ColdRecordBatch, String> {
        let cold_batch = std::mem::replace(
            &mut self.batch_builder,
            CleanColdRecordBatchBuilder::new(&self.parquet_columns)?,
        )
        .finish()?;
        Ok(cold_batch)
    }
}

/// Streams mirror rows through `fetch_batch` and invokes `write_chunk` per segment.
///
/// # Errors
///
/// Returns an error when selection planning, encoding, or a chunk write fails.
pub fn stream_flush_chunks<F, W>(
    input: &StreamEncodeInput,
    mut fetch_batch: F,
    mut write_chunk: W,
) -> Result<StreamEncodeOutcome, String>
where
    F: FnMut(&SqlStatement, i64, i64) -> Result<Vec<FlushMirrorRow>, String>,
    W: FnMut(FlushWriteChunk) -> Result<(), String>,
{
    let selection = crate::ops::plan_mirror_flush_selection_batch_with_order_key(
        &input.table,
        &input.mirror,
        &input.primary_key_columns,
        &input.base_column_names,
        None,
        input.mirror_ops.as_deref(),
        input.sort_by_order_key,
    )
    .map_err(|error| error.to_string())?;

    let mut after_seq = 0_i64;
    let mut rows_written = 0_usize;
    let mut max_seq = 0_i64;
    let mut chunk_builder = ChunkBuilder::new(&input.parquet_columns)?;
    let mut segment_builder = SegmentBuilder::new(input);
    let pk_indices =
        koldstore_parquet::pk_column_indices(&input.base_column_names, &input.primary_key_columns)?;
    let mut ordered_rows = Vec::new();

    loop {
        let batch = fetch_batch(&selection.statement, input.max_seq, after_seq)?;
        if batch.is_empty() {
            break;
        }
        after_seq = batch.last().map(|row| row.seq).unwrap_or(after_seq);
        max_seq = after_seq;
        let batch_len = batch.len();
        if input.sort_by_order_key {
            ordered_rows.extend(batch);
        } else {
            for row in batch {
                chunk_builder.push_row(&row, &input.primary_key_columns, input.schema_version)?;
                rows_written += 1;
                let row_group_limit = input.row_group_size.max(1).min(
                    segment_builder
                        .remaining_rows(input.max_rows_per_file)
                        .max(1),
                );
                if chunk_builder.len() >= row_group_limit {
                    let cold_batch = chunk_builder.take_batch()?;
                    if segment_builder.push_batch(cold_batch)? {
                        if let Some(chunk) = segment_builder.finish_segment()? {
                            write_chunk(chunk)?;
                        }
                    }
                }
            }
        }
        if (batch_len as i64) < input.fetch_batch_size {
            break;
        }
    }

    if input.sort_by_order_key {
        if ordered_rows.iter().any(|row| row.order_key.is_none()) {
            return Err("configured segment order key is missing from a mirror row".to_string());
        }
        sort_flush_rows(&mut ordered_rows, &pk_indices);
        for row in ordered_rows {
            chunk_builder.push_row(&row, &input.primary_key_columns, input.schema_version)?;
            rows_written += 1;
            let row_group_limit = input.row_group_size.max(1).min(
                segment_builder
                    .remaining_rows(input.max_rows_per_file)
                    .max(1),
            );
            if chunk_builder.len() >= row_group_limit {
                let cold_batch = chunk_builder.take_batch()?;
                if segment_builder.push_batch(cold_batch)? {
                    if let Some(chunk) = segment_builder.finish_segment()? {
                        write_chunk(chunk)?;
                    }
                }
            }
        }
    }

    if chunk_builder.len() > 0 {
        let cold_batch = chunk_builder.take_batch()?;
        let _ = segment_builder.push_batch(cold_batch)?;
    }
    if let Some(chunk) = segment_builder.finish_segment()? {
        write_chunk(chunk)?;
    }

    Ok(StreamEncodeOutcome {
        max_seq,
        rows_written,
    })
}

fn sort_flush_rows(rows: &mut [FlushMirrorRow], primary_key_indices: &[usize]) {
    rows.sort_by(|left, right| {
        left.order_key
            .as_deref()
            .cmp(&right.order_key.as_deref())
            .then_with(|| {
                for index in primary_key_indices {
                    let ordering =
                        compare_flush_values(left.values.get(*index), right.values.get(*index));
                    if !ordering.is_eq() {
                        return ordering;
                    }
                }
                left.seq.cmp(&right.seq)
            })
    });
}

fn compare_flush_values(
    left: Option<&koldstore_common::CellValue>,
    right: Option<&koldstore_common::CellValue>,
) -> std::cmp::Ordering {
    use koldstore_common::CellValue;
    match (left, right) {
        (Some(CellValue::Bool(left)), Some(CellValue::Bool(right))) => left.cmp(right),
        (Some(CellValue::Int16(left)), Some(CellValue::Int16(right))) => left.cmp(right),
        (Some(CellValue::Int32(left)), Some(CellValue::Int32(right))) => left.cmp(right),
        (Some(CellValue::Int64(left)), Some(CellValue::Int64(right))) => left.cmp(right),
        (Some(CellValue::Float32(left)), Some(CellValue::Float32(right))) => left.total_cmp(right),
        (Some(CellValue::Float64(left)), Some(CellValue::Float64(right))) => left.total_cmp(right),
        (Some(CellValue::Utf8(left)), Some(CellValue::Utf8(right))) => left.cmp(right),
        (Some(CellValue::TimestamptzMicros(left)), Some(CellValue::TimestamptzMicros(right))) => {
            left.cmp(right)
        }
        (Some(CellValue::Null), Some(CellValue::Null)) | (None, None) => std::cmp::Ordering::Equal,
        (Some(CellValue::Null) | None, _) => std::cmp::Ordering::Less,
        (_, Some(CellValue::Null) | None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => format!("{left:?}").cmp(&format!("{right:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use koldstore_common::{ColumnId, ColumnRef};
    use koldstore_parquet::{CellValue, PgType};
    use koldstore_sortkey::{decode_sort_key, SortKeyType, SortKeyValue};

    fn input(target_file_size_bytes: Option<u64>, max_rows_per_file: usize) -> StreamEncodeInput {
        StreamEncodeInput {
            table: QualifiedTableName::parse("app.items").unwrap(),
            mirror: QualifiedTableName::parse("koldstore.items__cl").unwrap(),
            primary_key_columns: vec!["id".to_string()],
            base_column_names: vec!["id".to_string(), "body".to_string()],
            parquet_columns: vec![
                PgColumn::new("id", PgType::Int8, false),
                PgColumn::new("body", PgType::Text, true),
            ],
            indexed_columns: vec![ColumnRef::new(ColumnId::from_attnum(1), "id")],
            schema_version: 1,
            max_seq: 5,
            max_rows_per_file,
            fetch_batch_size: i64::try_from(max_rows_per_file.max(1)).unwrap_or(1),
            target_file_size_bytes,
            compression: "zstd".to_string(),
            row_group_size: 1,
            mirror_ops: None,
            sort_by_order_key: false,
            order_key_column: None,
        }
    }

    fn rows() -> Vec<FlushMirrorRow> {
        (1..=5)
            .map(|seq| FlushMirrorRow {
                seq,
                op: 1,
                values: vec![CellValue::Int64(seq), CellValue::Utf8("payload".repeat(20))],
                order_key: None,
            })
            .collect()
    }

    fn run(input: StreamEncodeInput) -> (StreamEncodeOutcome, Vec<usize>) {
        let mut fetched = false;
        let mut segment_rows = Vec::new();
        let outcome = stream_flush_chunks(
            &input,
            |_, _, _| {
                if fetched {
                    Ok(Vec::new())
                } else {
                    fetched = true;
                    Ok(rows())
                }
            },
            |chunk| {
                segment_rows.push(chunk.row_count());
                Ok(())
            },
        )
        .unwrap();
        (outcome, segment_rows)
    }

    #[test]
    fn row_cap_splits_only_the_selected_rows() {
        let (outcome, segment_rows) = run(input(None, 2));

        assert_eq!(outcome.rows_written, 5);
        assert_eq!(segment_rows, vec![2, 2, 1]);
    }

    #[test]
    fn compressed_size_target_closes_segments_without_filling_past_selection() {
        let (outcome, segment_rows) = run(input(Some(1), 100));

        assert_eq!(outcome.rows_written, 5);
        assert_eq!(segment_rows, vec![1, 1, 1, 1, 1]);
    }

    #[test]
    fn streamed_chunk_carries_footer_derived_packed_metadata() {
        let mut encode_input = input(None, 100);
        encode_input.row_group_size = 2;
        let mut fetched = false;
        let mut packed = None;

        stream_flush_chunks(
            &encode_input,
            |_, _, _| {
                if fetched {
                    Ok(Vec::new())
                } else {
                    fetched = true;
                    Ok(rows())
                }
            },
            |chunk| {
                packed = Some(chunk.packed_metadata.clone());
                Ok(())
            },
        )
        .unwrap();

        let packed = packed.unwrap();
        assert_eq!(packed.row_group_count, 3);
        assert_eq!(packed.row_group_row_counts, vec![2, 2, 1]);
        assert_eq!(packed.row_group_min_seqs, vec![1, 3, 5]);
        assert_eq!(packed.row_group_max_seqs, vec![2, 4, 5]);
        let id = &packed.column_indexes[0];
        assert_eq!(id.column_id, ColumnId::from_attnum(1));
        assert_eq!(
            id.row_group_min_values
                .iter()
                .map(|value| {
                    decode_sort_key(SortKeyType::Int8, value.as_deref().unwrap()).unwrap()
                })
                .collect::<Vec<_>>(),
            vec![
                SortKeyValue::Int8(1),
                SortKeyValue::Int8(3),
                SortKeyValue::Int8(5),
            ]
        );
    }

    #[test]
    fn segment_order_sort_uses_primary_key_as_tie_breaker() {
        let mut rows = vec![
            FlushMirrorRow {
                seq: 1,
                op: 1,
                values: vec![CellValue::Int64(3), CellValue::Int64(10)],
                order_key: Some(vec![10]),
            },
            FlushMirrorRow {
                seq: 2,
                op: 1,
                values: vec![CellValue::Int64(2), CellValue::Int64(10)],
                order_key: Some(vec![10]),
            },
            FlushMirrorRow {
                seq: 3,
                op: 1,
                values: vec![CellValue::Int64(1), CellValue::Int64(5)],
                order_key: Some(vec![5]),
            },
        ];

        sort_flush_rows(&mut rows, &[0]);
        assert_eq!(
            rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
    }
}
