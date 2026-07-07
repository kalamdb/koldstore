//! Arrow/Parquet schema, reader, writer, footer, and pruning helpers.

pub mod footer;
pub mod pg_type_codec;
pub mod prune;
pub mod reader;
pub mod schema;
pub mod writer;

pub use koldstore_common::canonical_postgres_type_name;
pub use koldstore_schema::{PgIntegerArrayOid, PgType, SchemaError};
pub use pg_type_codec::{
    arrow_array_for_column, arrow_array_from_json, arrow_data_type, json_from_arrow_cell,
    json_bool, json_i16, json_i64, json_u32, json_value_from_arrow_column,
};
pub use schema::{
    build_clean_arrow_schema, ColdMetadataColumn, PgColumn,
};
pub use footer::{ColumnStats, FooterSummary, RowGroupStats, SegmentFooterMetadata};
pub use prune::{PruneDecision, RowGroupPruner};
pub use reader::{
    read_clean_cold_rows_from_path, CleanColdRow, ParquetReadOptions, ParquetReadRequest,
    RecordBatchFileStream,
};
pub use writer::{
    plan_clean_cold_record, record_batch_from_clean_cold_records, CleanColdRecordPlan,
    ParquetSegmentWriter, SegmentMetadataInput, SegmentWritePlan, StreamingRowGroupPlan,
    WriterOptions,
};
