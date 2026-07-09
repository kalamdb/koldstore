//! Flush write chunk boundary between mirror encoding and segment writes.
//!
//! Owns the PG-free type passed from row encoding to Parquet segment emission.
//! SPI fetch, typed decode, and Arrow batch building stay in `pg_koldstore`.

pub use koldstore_parquet::ColdRecordBatch;

/// One bounded Parquet segment chunk produced during flush encoding.
#[derive(Debug, Clone)]
pub struct FlushWriteChunk {
    /// Arrow batch and chunk stats ready for Parquet encoding.
    pub cold_batch: ColdRecordBatch,
}
