//! Global Parquet reader slot planning for cold reads.
//!
//! PostgreSQL backends coordinate reader pressure with advisory-lock slots.
//! This module owns the stable lock namespace and pure validation helpers used
//! by both runtime code and tests.

use crate::settings;

/// Advisory lock namespace for KoldStore Parquet reader slots.
pub const READER_LOCK_NAMESPACE: i32 = 0x4b52_5044;

/// Two-int advisory lock key for one Parquet reader slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParquetReaderLockKey(pub i32, pub i32);

/// Clamps the configured reader count to the supported concurrency range.
#[must_use]
pub const fn validated_max_open_parquet_readers(value: i32) -> i32 {
    settings::bounded_concurrency_limit(value)
}

/// Returns the advisory lock key for a bounded reader slot.
#[must_use]
pub fn parquet_reader_lock_key(slot: i32) -> ParquetReaderLockKey {
    ParquetReaderLockKey(READER_LOCK_NAMESPACE, slot)
}
