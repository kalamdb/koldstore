//! Arrow/Parquet schema, reader, writer, footer, and pruning helpers.

pub mod footer;
pub mod prune;
pub mod reader;
pub mod schema;
pub mod writer;

pub use footer::{ColumnStats, FooterSummary, RowGroupStats, SegmentFooterMetadata};
pub use prune::{PruneDecision, RowGroupPruner};
pub use reader::{ParquetReadOptions, ParquetReadRequest, RecordBatchFileStream};
pub use schema::{build_arrow_schema, PgColumn, PgType, SchemaError, SystemColumn};
pub use writer::{
    ParquetSegmentWriter, SegmentMetadataInput, SegmentWritePlan, StreamingRowGroupPlan,
    WriterOptions,
};
