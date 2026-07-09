//! Per-backend Parquet reader concurrency limits for cold reads.
//!
//! Caps open readers inside one PostgreSQL backend. Unlike the previous
//! cluster-wide advisory-lock pool, this never sleeps across backends.

use std::cell::Cell;

use crate::settings;

thread_local! {
    static OPEN_READERS: Cell<i32> = const { Cell::new(0) };
}

/// Clamps the configured reader count to the supported concurrency range.
#[must_use]
pub const fn validated_max_open_parquet_readers(value: i32) -> i32 {
    settings::bounded_concurrency_limit(value)
}

/// RAII permit for one open Parquet reader in the current backend.
#[derive(Debug)]
pub struct ParquetReaderPermit;

impl Drop for ParquetReaderPermit {
    fn drop(&mut self) {
        OPEN_READERS.with(|count| {
            let current = count.get();
            count.set(current.saturating_sub(1));
        });
    }
}

/// Acquires one reader permit or fails immediately when the backend is at capacity.
///
/// # Errors
///
/// Returns an error when `koldstore.max_open_parquet_readers` permits are already held
/// in this backend.
pub fn try_acquire_parquet_reader_permit(
    configured_limit: i32,
) -> Result<ParquetReaderPermit, String> {
    let max_open = validated_max_open_parquet_readers(configured_limit);
    OPEN_READERS.with(|count| {
        let current = count.get();
        if current >= max_open {
            return Err(format!(
                "backend already holds {current} open Parquet readers (limit {max_open}); \
                 raise koldstore.max_open_parquet_readers or reduce concurrent cold segments"
            ));
        }
        count.set(current + 1);
        Ok(ParquetReaderPermit)
    })
}

/// Returns the number of open reader permits in this backend (test helper).
#[must_use]
pub fn open_reader_count() -> i32 {
    OPEN_READERS.with(Cell::get)
}
