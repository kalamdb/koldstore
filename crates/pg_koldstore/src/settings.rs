//! Central pg-koldstore runtime settings.
//!
//! Owns GUC names, defaults, and typed validation used by SQL-facing code and
//! pure planning tests. PostgreSQL registration lives in `guc.rs`.

/// Default cold-read mode.
pub const DEFAULT_COLD_READS: &str = "auto";
/// Default maximum concurrently running background jobs.
pub const DEFAULT_MAX_RUNNING_JOBS: i32 = 4;
/// Default maximum globally open Parquet readers.
pub const DEFAULT_MAX_OPEN_PARQUET_READERS: i32 = 32;
/// Default extension log level.
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// Minimum accepted integer setting value for concurrency limits.
pub const MIN_CONCURRENCY_LIMIT: i32 = 1;
/// Conservative hard cap to avoid unbounded backend memory or object-store pressure.
pub const MAX_CONCURRENCY_LIMIT: i32 = 1024;

/// Names of public GUCs owned by pg-koldstore.
pub const COLD_READS_GUC: &str = "koldstore.cold_reads";
pub const MAX_OPEN_PARQUET_READERS_GUC: &str = "koldstore.max_open_parquet_readers";
pub const MAX_RUNNING_JOBS_GUC: &str = "koldstore.max_running_jobs";
pub const LOG_LEVEL_GUC: &str = "koldstore.log_level";
/// GUC that sets the minimum allowed `max_rows_per_file` for managed tables.
pub const MIN_MAX_ROWS_PER_FILE_GUC: &str = "koldstore.min_max_rows_per_file";
/// Test-only failpoint arming GUC (empty = disabled).
pub const FAILPOINT_GUC: &str = "koldstore.failpoint";
/// Default failpoint value (disabled).
pub const DEFAULT_FAILPOINT: &str = "";

/// TTL for `pending` cold segments before recover_segments expires them.
pub const PENDING_SEGMENT_TTL_SECONDS_GUC: &str = "koldstore.pending_segment_ttl_seconds";
/// Default pending-segment TTL (1 hour).
pub const DEFAULT_PENDING_SEGMENT_TTL_SECONDS: i32 = 3600;
/// Minimum pending-segment TTL (allow short values in tests).
pub const MIN_PENDING_SEGMENT_TTL_SECONDS: i32 = 1;
/// Maximum pending-segment TTL (30 days).
pub const MAX_PENDING_SEGMENT_TTL_SECONDS: i32 = 30 * 24 * 3600;

/// How often the database worker evaluates auto-flush eligibility.
pub const FLUSH_CHECK_INTERVAL_SECONDS_GUC: &str = "koldstore.flush_check_interval_seconds";
/// Default flush-check cadence (30 seconds).
pub const DEFAULT_FLUSH_CHECK_INTERVAL_SECONDS: i32 = 30;
/// Minimum flush-check interval.
pub const MIN_FLUSH_CHECK_INTERVAL_SECONDS: i32 = 1;
/// Maximum flush-check interval (1 day).
pub const MAX_FLUSH_CHECK_INTERVAL_SECONDS: i32 = 24 * 3600;

/// Latch poll interval for the async mirror apply loop (milliseconds).
pub const ASYNC_APPLY_POLL_INTERVAL_MS_GUC: &str = "koldstore.async_apply_poll_interval_ms";
/// Default apply latch poll cadence (100 ms).
pub const DEFAULT_ASYNC_APPLY_POLL_INTERVAL_MS: i32 = 100;
/// Minimum apply poll interval (avoids busy-spin).
pub const MIN_ASYNC_APPLY_POLL_INTERVAL_MS: i32 = 50;
/// Maximum apply poll interval (5 seconds).
pub const MAX_ASYNC_APPLY_POLL_INTERVAL_MS: i32 = 5_000;

/// Per-tick row budget for bounded async apply (0 = unlimited within the tick).
pub const ASYNC_APPLY_MAX_ROWS_PER_TICK_GUC: &str = "koldstore.async_apply_max_rows_per_tick";
/// Default: drain available WAL in one tick (compatibility with prior behavior).
pub const DEFAULT_ASYNC_APPLY_MAX_ROWS_PER_TICK: i32 = 0;
/// Minimum rows-per-tick (0 disables the row budget).
pub const MIN_ASYNC_APPLY_MAX_ROWS_PER_TICK: i32 = 0;
/// Hard cap on rows processed in one apply tick.
pub const MAX_ASYNC_APPLY_MAX_ROWS_PER_TICK: i32 = 1_000_000;

/// Per-tick wall-time budget for bounded async apply (0 = unlimited).
pub const ASYNC_APPLY_MAX_MS_PER_TICK_GUC: &str = "koldstore.async_apply_max_ms_per_tick";
/// Default: no time budget (compatibility with prior drain-all behavior).
pub const DEFAULT_ASYNC_APPLY_MAX_MS_PER_TICK: i32 = 0;
/// Minimum ms-per-tick (0 disables the time budget).
pub const MIN_ASYNC_APPLY_MAX_MS_PER_TICK: i32 = 0;
/// Hard cap on wall time for one apply tick.
pub const MAX_ASYNC_APPLY_MAX_MS_PER_TICK: i32 = 60_000;

/// Maximum bounded apply passes during flush phase-5.5 pre-lock catch-up.
pub const FLUSH_PRELOCK_MAX_PASSES_GUC: &str = "koldstore.flush_prelock_max_passes";
pub const DEFAULT_FLUSH_PRELOCK_MAX_PASSES: i32 = 3;
pub const MIN_FLUSH_PRELOCK_MAX_PASSES: i32 = 1;
pub const MAX_FLUSH_PRELOCK_MAX_PASSES: i32 = 16;

/// Wall-clock budget (ms) for all phase-5.5 pre-lock passes combined.
pub const FLUSH_PRELOCK_MAX_MS_GUC: &str = "koldstore.flush_prelock_max_ms";
pub const DEFAULT_FLUSH_PRELOCK_MAX_MS: i32 = 5_000;
pub const MIN_FLUSH_PRELOCK_MAX_MS: i32 = 100;
pub const MAX_FLUSH_PRELOCK_MAX_MS: i32 = 120_000;

/// Optional admission limit on logical-slot retained WAL bytes (`0` = disabled).
/// When exceeded, apply/flush fail closed — WAL is never silently dropped.
pub const ASYNC_MIRROR_MAX_RETAINED_BYTES_GUC: &str = "koldstore.async_mirror_max_retained_bytes";
pub const DEFAULT_ASYNC_MIRROR_MAX_RETAINED_BYTES: i32 = 0;
pub const MIN_ASYNC_MIRROR_MAX_RETAINED_BYTES: i32 = 0;
/// 64 GiB hard cap for the GUC (admission still never drops WAL).
pub const MAX_ASYNC_MIRROR_MAX_RETAINED_BYTES: i32 = i32::MAX;

/// Minimum accepted integer setting value for `min_max_rows_per_file`.
pub const MIN_MIN_MAX_ROWS_PER_FILE: i32 = 1;
/// Conservative hard cap for `min_max_rows_per_file`.
pub const MAX_MIN_MAX_ROWS_PER_FILE: i32 = 1_000_000;

/// Default minimum allowed `max_rows_per_file` for managed tables.
///
/// Kept in sync with [`koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE`].
pub const DEFAULT_MIN_MAX_ROWS_PER_FILE_SETTING: i32 =
    koldstore_common::DEFAULT_MIN_MAX_ROWS_PER_FILE as i32;

/// Runtime mode for cold reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdReadsMode {
    /// Planner/runtime decides when cold reads are required.
    Auto,
    /// Cold reads are allowed whenever needed.
    On,
    /// Cold reads fail closed when cold segments are required.
    Off,
}

impl ColdReadsMode {
    /// Parses a cold-read mode from GUC text.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "on" => Some(Self::On),
            "off" => Some(Self::Off),
            _ => None,
        }
    }

    /// Returns the canonical GUC text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::On => "on",
            Self::Off => "off",
        }
    }
}

/// Validates and clamps a concurrency setting.
#[must_use]
pub const fn bounded_concurrency_limit(value: i32) -> i32 {
    if value < MIN_CONCURRENCY_LIMIT {
        MIN_CONCURRENCY_LIMIT
    } else if value > MAX_CONCURRENCY_LIMIT {
        MAX_CONCURRENCY_LIMIT
    } else {
        value
    }
}

/// Validates and clamps the configured `max_rows_per_file` floor.
#[must_use]
pub const fn bounded_min_max_rows_per_file(value: i32) -> i32 {
    if value < MIN_MIN_MAX_ROWS_PER_FILE {
        MIN_MIN_MAX_ROWS_PER_FILE
    } else if value > MAX_MIN_MAX_ROWS_PER_FILE {
        MAX_MIN_MAX_ROWS_PER_FILE
    } else {
        value
    }
}

/// Default minimum allowed `max_rows_per_file` for managed tables.
#[must_use]
pub const fn default_min_max_rows_per_file() -> i32 {
    DEFAULT_MIN_MAX_ROWS_PER_FILE_SETTING
}
