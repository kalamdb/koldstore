//! Mirror-row streaming encoder: typed rows → Arrow chunks.
//!
//! SPI fetch stays in `pg_koldstore`; this module owns the PG-free encode loop.
//! Post-flush cleanup uses a seq-range DELETE (see `cleanup::plan_seq_range_cleanup`)
//! so this path no longer materializes per-row cleanup JSON.
//!
//! Ordered flush relies on PostgreSQL `ORDER BY order_key, PK…, seq` plus a matching
//! keyset page cursor so this crate never accumulates the full pass in a `Vec`.

use koldstore_common::{CellValue, ColumnRef, QualifiedTableName, SqlParamType, SqlStatement};
use koldstore_parquet::{
    extract_packed_segment_metadata, CleanColdRecordBatchBuilder, ColdMetadataColumn,
    ColdRecordBatch, FlushMirrorRow, PgColumn, SegmentSplitPolicy, SortingColumnSpec,
    StreamingParquetSegmentWriter, WriterOptions,
};

use crate::write::FlushWriteChunk;

/// Builds Parquet writer options (stats + Bloom) for one encode pass.
#[must_use]
pub fn writer_options_from_encode_input(input: &StreamEncodeInput) -> WriterOptions {
    WriterOptions {
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
    .with_bloom_filter_columns(
        input
            .bloom_filter_columns
            .iter()
            .cloned()
            .chain(input.primary_key_columns.iter().cloned()),
    )
}

/// Input for one streaming flush encode pass.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEncodeInput {
    /// Managed user table.
    pub table: QualifiedTableName,
    /// Mirror table for the managed table.
    pub mirror: QualifiedTableName,
    /// Primary-key column names.
    pub primary_key_columns: Vec<String>,
    /// SPI bind types aligned with [`Self::primary_key_columns`] (ordered keyset).
    pub primary_key_param_types: Vec<SqlParamType>,
    /// Application column names in catalog order.
    pub base_column_names: Vec<String>,
    /// Parquet schema columns.
    pub parquet_columns: Vec<PgColumn>,
    /// Indexed columns tracked for segment stats.
    pub indexed_columns: Vec<ColumnRef>,
    /// Columns that receive native Parquet Bloom filters (must include PK).
    pub bloom_filter_columns: Vec<String>,
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
    /// Ask PostgreSQL to return rows ordered by mirror `order_key`, then PK, then seq.
    pub sort_by_order_key: bool,
    /// Application column name for [`Self::sort_by_order_key`] (Parquet sort metadata).
    pub order_key_column: Option<String>,
}

/// Exclusive lower-bound cursor for one mirror flush page fetch.
#[derive(Debug, Clone, PartialEq)]
pub enum MirrorFlushPageCursor {
    /// Unordered / seq-ordered path: continue after this mirror `seq`.
    AfterSeq {
        /// Exclusive lower bound on `seq` (`0` starts the scan).
        after_seq: i64,
    },
    /// Ordered path: continue after `(order_key, primary key…, seq)`.
    AfterOrderKey {
        /// When `None`, fetch the first page (SQL first-page flag).
        after_order_key: Option<Vec<u8>>,
        /// Primary-key values from the previous page's last row (catalog PK order).
        after_pk_values: Vec<CellValue>,
        /// `seq` from the previous page's last row (ignored on the first page).
        after_seq: i64,
    },
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
        // Mirror fetch / encode always writes ascending `seq` for unsorted passes,
        // and still records seq as a trailing sort key for ordered passes.
        sorting_columns.push(SortingColumnSpec::ascending(ColdMetadataColumn::Seq.name()));

        let options = writer_options_from_encode_input(input).with_sorting_columns(sorting_columns);
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
/// Ordered mode never buffers the full selection in Rust: PostgreSQL returns pages
/// already sorted, and each page is encoded immediately.
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
    F: FnMut(&SqlStatement, i64, &MirrorFlushPageCursor) -> Result<Vec<FlushMirrorRow>, String>,
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
        &input.primary_key_param_types,
    )
    .map_err(|error| error.to_string())?;

    let pk_indices =
        koldstore_parquet::pk_column_indices(&input.base_column_names, &input.primary_key_columns)?;
    let mut cursor = if input.sort_by_order_key {
        MirrorFlushPageCursor::AfterOrderKey {
            after_order_key: None,
            after_pk_values: Vec::new(),
            after_seq: 0,
        }
    } else {
        MirrorFlushPageCursor::AfterSeq { after_seq: 0 }
    };
    let mut rows_written = 0_usize;
    let mut max_seq = 0_i64;
    let mut chunk_builder = ChunkBuilder::new(&input.parquet_columns)?;
    let mut segment_builder = SegmentBuilder::new(input);

    loop {
        let batch = fetch_batch(&selection.statement, input.max_seq, &cursor)?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        if input.sort_by_order_key && batch.iter().any(|row| row.order_key.is_none()) {
            return Err("configured segment order key is missing from a mirror row".to_string());
        }
        for row in &batch {
            max_seq = max_seq.max(row.seq);
            chunk_builder.push_row(row, &input.primary_key_columns, input.schema_version)?;
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
        let Some(last) = batch.last() else {
            break;
        };
        cursor = if input.sort_by_order_key {
            let after_pk_values = pk_indices
                .iter()
                .map(|index| last.values.get(*index).cloned().unwrap_or(CellValue::Null))
                .collect();
            MirrorFlushPageCursor::AfterOrderKey {
                after_order_key: last.order_key.clone(),
                after_pk_values,
                after_seq: last.seq,
            }
        } else {
            MirrorFlushPageCursor::AfterSeq {
                after_seq: last.seq,
            }
        };
        if (batch_len as i64) < input.fetch_batch_size {
            break;
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
            primary_key_param_types: vec![SqlParamType::BigInt],
            base_column_names: vec!["id".to_string(), "body".to_string()],
            parquet_columns: vec![
                PgColumn::new("id", PgType::Int8, false),
                PgColumn::new("body", PgType::Text, true),
            ],
            indexed_columns: vec![ColumnRef::new(ColumnId::from_attnum(1), "id")],
            bloom_filter_columns: vec!["id".to_string()],
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
    fn writer_options_include_configured_bloom_columns_and_force_pk() {
        let mut encode_input = input(None, 10);
        encode_input.bloom_filter_columns = vec!["body".to_string()];
        let options = writer_options_from_encode_input(&encode_input);
        assert_eq!(options.bloom_filter_columns, vec!["body", "id"]);
        assert!(options.statistics_columns.contains(&"seq".to_string()));
        assert!(options.statistics_columns.contains(&"id".to_string()));
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
    fn ordered_flush_streams_pages_without_buffering_all_rows() {
        let mut encode_input = input(None, 100);
        encode_input.sort_by_order_key = true;
        encode_input.order_key_column = Some("body".to_string());
        encode_input.fetch_batch_size = 1;
        encode_input.max_seq = 3;

        // Postgres-ordered pages of size 1: order_key 5 then 10/pk2 then 10/pk3.
        let pages = [
            FlushMirrorRow {
                seq: 3,
                op: 1,
                values: vec![CellValue::Int64(1), CellValue::Utf8("a".into())],
                order_key: Some(vec![5]),
            },
            FlushMirrorRow {
                seq: 2,
                op: 1,
                values: vec![CellValue::Int64(2), CellValue::Utf8("b".into())],
                order_key: Some(vec![10]),
            },
            FlushMirrorRow {
                seq: 1,
                op: 1,
                values: vec![CellValue::Int64(3), CellValue::Utf8("c".into())],
                order_key: Some(vec![10]),
            },
        ];
        let mut page_idx = 0_usize;
        let mut seen_cursors = Vec::new();
        let mut peak_batch = 0_usize;

        let outcome = stream_flush_chunks(
            &encode_input,
            |statement, max_seq, cursor| {
                assert_eq!(max_seq, 3);
                assert!(statement.sql.contains("ORDER BY mirror.\"order_key\""));
                assert!(statement
                    .sql
                    .contains("($2::boolean OR (mirror.\"order_key\""));
                seen_cursors.push(cursor.clone());
                let batch = pages
                    .get(page_idx)
                    .cloned()
                    .map(|row| vec![row])
                    .unwrap_or_default();
                page_idx += 1;
                peak_batch = peak_batch.max(batch.len());
                Ok(batch)
            },
            |_| Ok(()),
        )
        .unwrap();

        assert_eq!(outcome.rows_written, 3);
        assert_eq!(outcome.max_seq, 3);
        assert_eq!(peak_batch, 1, "ordered path must stream one page at a time");
        assert!(seen_cursors.len() >= 3);
        assert!(matches!(
            seen_cursors.first(),
            Some(MirrorFlushPageCursor::AfterOrderKey {
                after_order_key: None,
                ..
            })
        ));
        assert!(matches!(
            seen_cursors.get(1),
            Some(MirrorFlushPageCursor::AfterOrderKey {
                after_order_key: Some(key),
                after_seq: 3,
                ..
            }) if key.as_slice() == [5]
        ));
    }

    #[test]
    fn ordered_flush_rejects_missing_order_key() {
        let mut encode_input = input(None, 100);
        encode_input.sort_by_order_key = true;
        encode_input.order_key_column = Some("body".to_string());

        let error = stream_flush_chunks(
            &encode_input,
            |_, _, _| {
                Ok(vec![FlushMirrorRow {
                    seq: 1,
                    op: 1,
                    values: vec![CellValue::Int64(1), CellValue::Utf8("x".into())],
                    order_key: None,
                }])
            },
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(error.contains("order key is missing"));
    }
}
