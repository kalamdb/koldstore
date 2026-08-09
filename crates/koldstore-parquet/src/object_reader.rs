//! ObjectStore-backed [`AsyncFileReader`] for footer-first Parquet reads.
//!
//! Mirrors kalamdb's `ParquetObjectReader` usage without depending on parquet's
//! `object_store` feature (which pins an older `object_store` crate). Only the
//! footer is fetched eagerly; column chunks and bloom pages use range GETs.

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

/// Optional I/O counters and wait timing for EXPLAIN diagnostics and tests.
#[derive(Debug, Default)]
pub struct ObjectStoreReadStats {
    /// Number of `get_range` / `get_ranges` / suffix `get_opts` calls.
    pub range_calls: AtomicU64,
    /// Total bytes returned by those range calls.
    pub bytes_read: AtomicU64,
    /// Wall time awaiting successful object-store range reads, in nanoseconds.
    read_nanos: AtomicU64,
    /// Whether callers requested wall-clock timing in addition to counters.
    timing_enabled: bool,
}

/// Point-in-time object-store I/O counters for one Parquet reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ObjectStoreReadSnapshot {
    /// Successful range/suffix calls completed so far.
    pub range_calls: u64,
    /// Bytes returned by successful range/suffix calls.
    pub bytes_read: u64,
    /// Wall time spent awaiting successful range/suffix calls.
    pub read_duration: Duration,
}

impl ObjectStoreReadStats {
    /// Creates counters that also measure object-store wait time.
    #[must_use]
    pub fn with_timing() -> Self {
        Self {
            timing_enabled: true,
            ..Self::default()
        }
    }

    /// Snapshot of `(range_calls, bytes_read)` for compatibility with existing callers.
    #[must_use]
    pub fn snapshot(&self) -> (u64, u64) {
        let snapshot = self.timed_snapshot();
        (snapshot.range_calls, snapshot.bytes_read)
    }

    /// Returns completed-read counters plus accumulated object-store wait time.
    #[must_use]
    pub fn timed_snapshot(&self) -> ObjectStoreReadSnapshot {
        ObjectStoreReadSnapshot {
            range_calls: self.range_calls.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            read_duration: Duration::from_nanos(self.read_nanos.load(Ordering::Relaxed)),
        }
    }

    fn start_timer(&self) -> Option<Instant> {
        self.timing_enabled.then(Instant::now)
    }

    fn record_read(&self, bytes: usize, started: Option<Instant>) {
        self.range_calls.fetch_add(1, Ordering::Relaxed);
        self.bytes_read
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
        if let Some(started) = started {
            let elapsed = started.elapsed();
            let elapsed_nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
            let _ = self
                .read_nanos
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_add(elapsed_nanos))
                });
        }
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
            let started = stats.as_ref().and_then(|stats| stats.start_timer());
            let bytes = store
                .get_range(&path, range)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            if let Some(stats) = stats {
                stats.record_read(bytes.len(), started);
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
            let started = stats.as_ref().and_then(|stats| stats.start_timer());
            let parts = store
                .get_ranges(&path, &ranges)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            if let Some(stats) = stats {
                let total = parts.iter().map(bytes::Bytes::len).sum();
                stats.record_read(total, started);
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
            let started = stats.as_ref().and_then(|stats| stats.start_timer());
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
                stats.record_read(bytes.len(), started);
            }
            Ok(bytes)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectStoreReadStats;
    use std::time::Duration;

    #[test]
    fn object_store_read_stats_do_not_measure_wait_time_by_default() {
        let stats = ObjectStoreReadStats::default();

        stats.record_read(512, stats.start_timer());
        stats.record_read(256, stats.start_timer());

        let snapshot = stats.timed_snapshot();
        assert_eq!(snapshot.range_calls, 2);
        assert_eq!(snapshot.bytes_read, 768);
        assert_eq!(snapshot.read_duration, Duration::ZERO);
    }

    #[test]
    fn object_store_read_stats_measure_wait_time_when_requested() {
        let stats = ObjectStoreReadStats::with_timing();

        let started = stats.start_timer().expect("timed stats must start a clock");
        while started.elapsed().is_zero() {
            std::hint::spin_loop();
        }
        stats.record_read(512, Some(started));

        let snapshot = stats.timed_snapshot();
        assert_eq!(snapshot.range_calls, 1);
        assert_eq!(snapshot.bytes_read, 512);
        assert!(snapshot.read_duration > Duration::ZERO);
    }
}
