//! Arrow/Parquet schema, reader, writer, footer, and pruning helpers.

pub mod batch_builder;
pub mod footer;
pub mod footer_cache;
pub mod object_reader;
pub mod pg_type_codec;
pub mod prune;
pub mod reader;
pub mod schema;
pub mod writer;

pub use batch_builder::{
    pk_column_indices, CleanColdRecordBatchBuilder, ColdRecordBatch, FlushColumnValue,
    FlushMirrorRow,
};
pub use footer::{
    extract_packed_segment_metadata, ColumnStats, FooterSummary, PackedColumnIndex,
    PackedSegmentMetadata, RowGroupStats, SegmentFooterMetadata,
};
pub use footer_cache as parquet_footer_cache;
pub use koldstore_common::canonical_postgres_type_name;
pub use koldstore_schema::{PgIntegerArrayOid, PgType, SchemaError};
pub use object_reader::{ObjectStoreParquetReader, ObjectStoreReadStats};
pub use pg_type_codec::{
    arrow_array_for_column, arrow_array_from_json, arrow_data_type, json_bool,
    json_from_arrow_cell, json_i16, json_i64, jsonb_cell_to_utf8, pg_bytea_hex,
};
pub use prune::{select_row_groups_for_pk_values, PruneDecision, RowGroupPruner};
pub use reader::{
    clean_cold_row_to_common, read_clean_cold_rows_from_object_store,
    read_clean_cold_rows_from_object_store_async, read_clean_cold_rows_from_object_store_with_size,
    read_clean_cold_rows_from_object_store_with_stats, read_clean_cold_rows_with_options,
    BloomPruneMode, CleanColdRow, ParquetReadOptions, ParquetReadProfile, ParquetReadRequest,
};
pub use schema::{build_clean_arrow_schema, ColdMetadataColumn, PgColumn};
pub use writer::{
    encode_parquet_segment_bytes, plan_clean_cold_record, record_batch_from_clean_cold_records,
    validate_finalized_parquet_segment, validate_parquet_bytes, CleanColdRecordPlan,
    EncodedParquetSegment, ParquetSegmentWriter, ParquetValidation, SegmentMetadataInput,
    SegmentSplitPolicy, SegmentWritePlan, StreamingParquetSegmentWriter, StreamingRowGroupPlan,
    WriterOptions,
};
