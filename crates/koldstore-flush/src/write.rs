//! Flush write chunk boundary between mirror encoding and segment writes.
//!
//! Owns the PG-free type passed from row encoding to Parquet segment emission.
//! SPI fetch, typed decode, and Arrow batch building stay in `pg_koldstore`.
//! Catalog column stats come from the encoded Parquet footer (not encode-time
//! bounds tracking).

use std::collections::BTreeMap;

use koldstore_common::ColumnId;
use koldstore_parquet::{catalog_stats_from_parquet_bytes, ColdRecordBatch, PgColumn};

/// One bounded, fully encoded Parquet segment produced during flush encoding.
#[derive(Debug, Clone)]
pub struct FlushWriteChunk {
    /// Complete Parquet bytes, including footer.
    pub parquet_bytes: Vec<u8>,
    /// Number of selected rows in this segment.
    pub row_count: usize,
    /// Minimum selected sequence.
    pub min_seq: i64,
    /// Maximum selected sequence.
    pub max_seq: i64,
    /// Footer-derived catalog stats keyed by [`ColumnId`].
    pub column_stats: BTreeMap<ColumnId, (serde_json::Value, serde_json::Value)>,
}

impl FlushWriteChunk {
    /// Builds a segment chunk from its encoded bytes and bounded Arrow batches.
    ///
    /// # Errors
    ///
    /// Returns an error when footer statistics cannot be read from `parquet_bytes`.
    pub fn from_encoded_batches(
        parquet_bytes: Vec<u8>,
        batches: &[ColdRecordBatch],
        stats_columns: &[PgColumn],
    ) -> Result<Self, String> {
        let row_count = batches.iter().map(|batch| batch.row_count).sum();
        let min_seq = batches.first().map_or(0, |batch| batch.min_seq);
        let max_seq = batches.last().map_or(0, |batch| batch.max_seq);
        let column_stats = catalog_stats_from_parquet_bytes(&parquet_bytes, stats_columns)?
            .into_iter()
            .map(|(column_id, stats)| (column_id, (stats.min, stats.max)))
            .collect();
        Ok(Self {
            parquet_bytes,
            row_count,
            min_seq,
            max_seq,
            column_stats,
        })
    }

    /// Returns the number of selected rows in this segment.
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
}
