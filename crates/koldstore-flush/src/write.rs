//! Flush write chunk boundary between mirror encoding and segment writes.
//!
//! Owns the PG-free type passed from row encoding to Parquet segment emission.
//! SPI fetch, typed decode, and Arrow batch building stay in `pg_koldstore`.

use std::collections::BTreeMap;

use koldstore_common::compare_json_values;
use koldstore_parquet::ColdRecordBatch;

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
    /// Combined indexed-column bounds.
    pub indexed_bounds: BTreeMap<String, (serde_json::Value, serde_json::Value)>,
}

impl FlushWriteChunk {
    /// Builds a segment chunk from its encoded bytes and bounded Arrow batches.
    #[must_use]
    pub fn from_encoded_batches(parquet_bytes: Vec<u8>, batches: &[ColdRecordBatch]) -> Self {
        let row_count = batches.iter().map(|batch| batch.row_count).sum();
        let min_seq = batches.first().map_or(0, |batch| batch.min_seq);
        let max_seq = batches.last().map_or(0, |batch| batch.max_seq);
        let mut indexed_bounds = BTreeMap::new();
        for batch in batches {
            for (column, (min, max)) in &batch.indexed_bounds {
                let bounds = indexed_bounds
                    .entry(column.clone())
                    .or_insert_with(|| (min.clone(), max.clone()));
                if compare_json_values(min, &bounds.0).is_some_and(|order| order.is_lt()) {
                    bounds.0 = min.clone();
                }
                if compare_json_values(max, &bounds.1).is_some_and(|order| order.is_gt()) {
                    bounds.1 = max.clone();
                }
            }
        }
        Self {
            parquet_bytes,
            row_count,
            min_seq,
            max_seq,
            indexed_bounds,
        }
    }

    /// Returns the number of selected rows in this segment.
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
}
