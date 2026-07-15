//! ObjectStore-backed [`AsyncFileReader`] for footer-first Parquet reads.
//!
//! Mirrors kalamdb's `ParquetObjectReader` usage without depending on parquet's
//! `object_store` feature (which pins an older `object_store` crate). Only the
//! footer is fetched eagerly; column chunks and bloom pages use range GETs.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

/// Optional I/O counters for proving range-only ObjectStore reads in tests.
#[derive(Debug, Default)]
pub struct ObjectStoreReadStats {
    /// Number of `get_range` / `get_ranges` / suffix `get_opts` calls.
    pub range_calls: AtomicU64,
    /// Total bytes returned by those range calls.
    pub bytes_read: AtomicU64,
}

impl ObjectStoreReadStats {
    /// Snapshot of `(range_calls, bytes_read)`.
    #[must_use]
    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.range_calls.load(Ordering::SeqCst),
            self.bytes_read.load(Ordering::SeqCst),
        )
    }
}

/// Range-request Parquet reader over any [`ObjectStore`] backend.
#[derive(Clone, Debug)]
pub struct ObjectStoreParquetReader {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    file_size: Option<u64>,
    metadata_size_hint: Option<usize>,
    stats: Option<Arc<ObjectStoreReadStats>>,
}

impl ObjectStoreParquetReader {
    /// Creates a reader for `path` in `store`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, path: ObjectPath) -> Self {
        Self {
            store,
            path,
            file_size: None,
            metadata_size_hint: None,
            stats: None,
        }
    }

    /// Parses `path` and creates a reader.
    ///
    /// # Errors
    ///
    /// Returns an error when `path` is not a valid object-store path.
    pub fn from_key(store: Arc<dyn ObjectStore>, path: &str) -> Result<Self, String> {
        let path = ObjectPath::parse(path).map_err(|error| error.to_string())?;
        Ok(Self::new(store, path))
    }

    /// Provides the object byte size so metadata loads use bounded ranges.
    #[must_use]
    pub fn with_file_size(mut self, file_size: u64) -> Self {
        self.file_size = Some(file_size);
        self
    }

    /// Hint for footer prefetch size.
    #[must_use]
    pub fn with_footer_size_hint(mut self, hint: usize) -> Self {
        self.metadata_size_hint = Some(hint);
        self
    }

    /// Attaches I/O counters (tests / diagnostics).
    #[must_use]
    pub fn with_stats(mut self, stats: Arc<ObjectStoreReadStats>) -> Self {
        self.stats = Some(stats);
        self
    }
}

impl AsyncFileReader for ObjectStoreParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let stats = self.stats.clone();
        async move {
            let bytes = store
                .get_range(&path, range)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            if let Some(stats) = stats {
                stats.range_calls.fetch_add(1, Ordering::SeqCst);
                stats
                    .bytes_read
                    .fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
            Ok(bytes)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let stats = self.stats.clone();
        async move {
            let parts = store
                .get_ranges(&path, &ranges)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            if let Some(stats) = stats {
                stats.range_calls.fetch_add(1, Ordering::SeqCst);
                let total: u64 = parts.iter().map(|b| b.len() as u64).sum();
                stats.bytes_read.fetch_add(total, Ordering::SeqCst);
            }
            Ok(parts)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            // Only cache footers loaded with page indexes skipped (the merge-scan
            // default). Indexed metadata is rarer and must not reuse a Skip entry.
            let indexes_requested = options.is_some_and(|opts| {
                opts.column_index_policy() != PageIndexPolicy::Skip
                    || opts.offset_index_policy() != PageIndexPolicy::Skip
            });
            let cache_path = self.path.as_ref().to_string();
            if !indexes_requested {
                if let Some(cached) = crate::footer_cache::get(&cache_path, self.file_size) {
                    return Ok(cached);
                }
            }

            let metadata_opts = options.map(|o| o.metadata_options().clone());
            let mut metadata = ParquetMetaDataReader::new()
                .with_metadata_options(metadata_opts)
                .with_column_index_policy(PageIndexPolicy::Skip)
                .with_offset_index_policy(PageIndexPolicy::Skip)
                .with_prefetch_hint(self.metadata_size_hint);

            if let Some(options) = options {
                if options.column_index_policy() != PageIndexPolicy::Skip
                    || options.offset_index_policy() != PageIndexPolicy::Skip
                {
                    metadata = metadata
                        .with_column_index_policy(options.column_index_policy())
                        .with_offset_index_policy(options.offset_index_policy());
                }
            }

            let file_size = self.file_size;
            let metadata = if let Some(file_size) = file_size {
                metadata.load_and_finish(self, file_size).await?
            } else {
                metadata.load_via_suffix_and_finish(self).await?
            };
            let metadata = Arc::new(metadata);
            if !indexes_requested {
                crate::footer_cache::insert(&cache_path, file_size, Arc::clone(&metadata));
            }
            Ok(metadata)
        })
    }
}

impl MetadataSuffixFetch for &mut ObjectStoreParquetReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let stats = self.stats.clone();
        async move {
            let options = GetOptions {
                range: Some(GetRange::Suffix(suffix as u64)),
                ..Default::default()
            };
            let resp = store
                .get_opts(&path, options)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            let bytes = resp
                .bytes()
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            if let Some(stats) = stats {
                stats.range_calls.fetch_add(1, Ordering::SeqCst);
                stats
                    .bytes_read
                    .fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
            Ok(bytes)
        }
        .boxed()
    }
}
