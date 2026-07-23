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
    /// Builds a segment chunk from already-aggregated encode metadata.
    ///
    /// Prefer this over retaining Arrow batches beside the Parquet buffer.
    #[must_use]
    pub fn from_parts(
        parquet_bytes: Vec<u8>,
        row_count: usize,
        min_seq: i64,
        max_seq: i64,
        indexed_bounds: BTreeMap<String, (serde_json::Value, serde_json::Value)>,
    ) -> Self {
        Self {
            parquet_bytes,
            row_count,
            min_seq,
            max_seq,
            indexed_bounds,
        }
    }

    /// Builds a segment chunk from its encoded bytes and bounded Arrow batches.
    ///
    /// Kept for tests/helpers that still materialize `ColdRecordBatch` lists.
    /// Production encode accumulates bounds incrementally and uses [`Self::from_parts`].
    #[must_use]
    pub fn from_encoded_batches(parquet_bytes: Vec<u8>, batches: &[ColdRecordBatch]) -> Self {
        let row_count = batches.iter().map(|batch| batch.row_count).sum();
        let min_seq = batches.first().map_or(0, |batch| batch.min_seq);
        let max_seq = batches.last().map_or(0, |batch| batch.max_seq);
        let mut indexed_bounds = BTreeMap::new();
        for batch in batches {
            merge_indexed_bounds(&mut indexed_bounds, &batch.indexed_bounds);
        }
        Self::from_parts(parquet_bytes, row_count, min_seq, max_seq, indexed_bounds)
    }

    /// Returns the number of selected rows in this segment.
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
}

/// Merges indexed-column min/max bounds from `incoming` into `target`.
pub fn merge_indexed_bounds(
    target: &mut BTreeMap<String, (serde_json::Value, serde_json::Value)>,
    incoming: &BTreeMap<String, (serde_json::Value, serde_json::Value)>,
) {
    for (column, (min, max)) in incoming {
        let bounds = target
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

#[cfg(test)]
mod tests {
    use super::{merge_indexed_bounds, FlushWriteChunk};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn from_parts_preserves_metadata_without_arrow_batches() {
        let chunk = FlushWriteChunk::from_parts(
            vec![1, 2, 3],
            10,
            1,
            10,
            BTreeMap::from([("id".to_string(), (json!(1), json!(10)))]),
        );
        assert_eq!(chunk.row_count(), 10);
        assert_eq!(chunk.min_seq, 1);
        assert_eq!(chunk.max_seq, 10);
        assert_eq!(chunk.parquet_bytes, vec![1, 2, 3]);
    }

    #[test]
    fn merge_indexed_bounds_expands_min_max() {
        let mut bounds = BTreeMap::from([("id".to_string(), (json!(5), json!(10)))]);
        merge_indexed_bounds(
            &mut bounds,
            &BTreeMap::from([("id".to_string(), (json!(1), json!(20)))]),
        );
        assert_eq!(bounds.get("id"), Some(&(json!(1), json!(20))));
    }
}
