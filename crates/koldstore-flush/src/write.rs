//! Flush write chunk boundary between mirror encoding and segment writes.
//!
//! Owns the PG-free type passed from row encoding to Parquet segment emission.
//! SPI fetch, typed decode, and Arrow batch building stay in `pg_koldstore`.

use std::sync::Arc;

use koldstore_parquet::{EncodedParquetSegment, PackedSegmentMetadata};

/// One bounded, fully encoded Parquet segment produced during flush encoding.
#[derive(Debug, Clone)]
pub struct FlushWriteChunk {
    /// Complete Parquet bytes, including footer.
    pub parquet_bytes: Vec<u8>,
    /// Exact finalized footer metadata describing `parquet_bytes`.
    pub parquet_metadata: Arc<parquet::file::metadata::ParquetMetaData>,
    /// Compact catalog metadata derived from `parquet_metadata`.
    pub packed_metadata: PackedSegmentMetadata,
}

impl FlushWriteChunk {
    /// Builds a segment chunk from finalized Parquet bytes and footer metadata.
    ///
    /// Prefer this over retaining Arrow batches beside the Parquet buffer.
    #[must_use]
    pub fn from_encoded(
        encoded: EncodedParquetSegment,
        packed_metadata: PackedSegmentMetadata,
    ) -> Self {
        Self {
            parquet_bytes: encoded.bytes,
            parquet_metadata: encoded.metadata,
            packed_metadata,
        }
    }

    /// Returns the number of selected rows in this segment.
    #[must_use]
    pub fn row_count(&self) -> usize {
        usize::try_from(self.packed_metadata.row_count).unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::FlushWriteChunk;
    use std::sync::Arc;

    #[test]
    fn chunk_retains_exact_footer_metadata_without_arrow_batches() {
        let batch = koldstore_parquet::record_batch_from_clean_cold_records(
            &[koldstore_parquet::PgColumn::new(
                "id",
                koldstore_parquet::PgType::Int8,
                false,
            )],
            &[koldstore_parquet::plan_clean_cold_record(
                [("id", serde_json::json!(1))],
                ["id"],
                1,
                1,
                1,
            )
            .unwrap()],
        )
        .unwrap();
        let encoded = koldstore_parquet::ParquetSegmentWriter::new(
            koldstore_parquet::WriterOptions::default().with_statistics_columns(["id", "seq"]),
        )
        .encode_record_batch_with_metadata(&batch)
        .unwrap();
        let metadata = koldstore_parquet::extract_packed_segment_metadata(
            encoded.metadata.as_ref(),
            &[koldstore_parquet::PgColumn::new(
                "id",
                koldstore_parquet::PgType::Int8,
                false,
            )],
            &[koldstore_common::ColumnRef::new(
                koldstore_common::ColumnId::from_attnum(1),
                "id",
            )],
            &["id".to_string()],
        )
        .unwrap();
        let expected = Arc::clone(&encoded.metadata);

        let chunk = FlushWriteChunk::from_encoded(encoded, metadata);

        assert_eq!(chunk.row_count(), 1);
        assert_eq!(chunk.packed_metadata.min_seq, 1);
        assert_eq!(chunk.packed_metadata.max_seq, 1);
        assert!(Arc::ptr_eq(&chunk.parquet_metadata, &expected));
    }
}
