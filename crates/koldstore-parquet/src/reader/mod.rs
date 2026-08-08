//! Direct ObjectStore-backed Parquet reader surface.
//!
//! Cold reads prefer [`read_clean_cold_rows_from_object_store`]: footer metadata
//! is loaded first via suffix/range GET, row groups are pruned (min/max + bloom),
//! then only selected column chunks are fetched. Local-path and in-memory helpers
//! remain for tests and flush validation.

mod decode;
mod local;
mod object_store;
mod options;
mod types;

pub use decode::clean_cold_row_to_common;
pub use local::read_clean_cold_rows_with_options;
pub use object_store::{
    read_clean_cold_rows_from_object_store, read_clean_cold_rows_from_object_store_async,
    read_clean_cold_rows_from_object_store_with_size,
    read_clean_cold_rows_from_object_store_with_stats,
};
pub use options::{
    BloomPruneMode, PageIndexPruneMode, ParquetReadOptions, ParquetReadProfile, PkValues, SeqRange,
};
pub use types::{CleanColdRow, ParquetReadRequest};
