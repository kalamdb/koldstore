//! Central pg-koldstore runtime settings.
//!
//! Owns GUC names, defaults, and typed validation used by SQL-facing code and
//! pure planning tests. PostgreSQL registration lives in `guc.rs`.

/// Default cold-read mode.
pub const DEFAULT_COLD_READS: &str = "auto";
/// Default maximum globally open Parquet readers.
pub const DEFAULT_MAX_OPEN_PARQUET_READERS: i32 = 32;
/// Default per-scan merge seen-key cap. Protects backends from accidental
/// full-table scans that retain millions of compact PK identities. `0` disables
/// the cap.
pub const DEFAULT_MAX_MERGE_SEEN_KEYS: i32 = 1_000_000;
/// Default ObjectStore / Parquet operation timeout (30 seconds).
pub const DEFAULT_OBJECT_STORE_TIMEOUT_MS: i32 = 30_000;
/// Default extension log level.
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// Minimum accepted integer setting value for concurrency limits.
pub const MIN_CONCURRENCY_LIMIT: i32 = 1;
/// Conservative hard cap to avoid unbounded backend memory or object-store pressure.
pub const MAX_CONCURRENCY_LIMIT: i32 = 1024;
/// Minimum merge seen-key cap (`0` = unlimited).
pub const MIN_MAX_MERGE_SEEN_KEYS: i32 = 0;
/// Hard cap for `koldstore.max_merge_seen_keys`.
pub const MAX_MAX_MERGE_SEEN_KEYS: i32 = 100_000_000;
/// Minimum object-store timeout (`0` = disabled).
pub const MIN_OBJECT_STORE_TIMEOUT_MS: i32 = 0;
/// Hard cap for `koldstore.object_store_timeout_ms` (10 minutes).
pub const MAX_OBJECT_STORE_TIMEOUT_MS: i32 = 600_000;

/// Names of public GUCs owned by pg-koldstore.
pub const COLD_READS_GUC: &str = "koldstore.cold_reads";
pub const MAX_OPEN_PARQUET_READERS_GUC: &str = "koldstore.max_open_parquet_readers";
pub const MAX_MERGE_SEEN_KEYS_GUC: &str = "koldstore.max_merge_seen_keys";
/// Wall-clock budget for one ObjectStore / Parquet segment operation.
pub const OBJECT_STORE_TIMEOUT_MS_GUC: &str = "koldstore.object_store_timeout_ms";
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

/// Safety watchdog for commit-driven async mirror wakeups (milliseconds).
pub const ASYNC_APPLY_WATCHDOG_INTERVAL_MS_GUC: &str = "koldstore.async_apply_watchdog_interval_ms";
/// Default watchdog cadence (30 seconds).
pub const DEFAULT_ASYNC_APPLY_WATCHDOG_INTERVAL_MS: i32 = 30_000;
/// Minimum watchdog interval (1 second).
pub const MIN_ASYNC_APPLY_WATCHDOG_INTERVAL_MS: i32 = 1_000;
/// Maximum watchdog interval (5 minutes).
pub const MAX_ASYNC_APPLY_WATCHDOG_INTERVAL_MS: i32 = 300_000;

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

/// Health threshold for logical-slot retained WAL bytes.
///
/// Exceeding it marks async status unhealthy but never blocks the applier that
/// can drain the slot. WAL is never silently dropped.
/// `0` disables the limit (not recommended for production async deployments).
pub const ASYNC_MIRROR_MAX_RETAINED_BYTES_GUC: &str = "koldstore.async_mirror_max_retained_bytes";
/// Default: 1 GiB. Protects `pg_wal` from unbounded slot retention when the
/// async apply worker stalls; operators can raise, lower, or set `0` to disable.
pub const DEFAULT_ASYNC_MIRROR_MAX_RETAINED_BYTES: i32 = 1_073_741_824;
pub const MIN_ASYNC_MIRROR_MAX_RETAINED_BYTES: i32 = 0;
/// Hard cap for the GUC (`i32::MAX` ≈ 2 GiB; monitoring never drops WAL).
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

/// Validates and clamps the per-scan merge seen-key cap (`0` stays unlimited).
#[must_use]
pub const fn bounded_max_merge_seen_keys(value: i32) -> i32 {
    if value < MIN_MAX_MERGE_SEEN_KEYS {
        MIN_MAX_MERGE_SEEN_KEYS
    } else if value > MAX_MAX_MERGE_SEEN_KEYS {
        MAX_MAX_MERGE_SEEN_KEYS
    } else {
        value
    }
}

/// Validates and clamps the ObjectStore operation timeout (`0` stays disabled).
#[must_use]
pub const fn bounded_object_store_timeout_ms(value: i32) -> i32 {
    if value < MIN_OBJECT_STORE_TIMEOUT_MS {
        MIN_OBJECT_STORE_TIMEOUT_MS
    } else if value > MAX_OBJECT_STORE_TIMEOUT_MS {
        MAX_OBJECT_STORE_TIMEOUT_MS
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
