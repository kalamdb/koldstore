//! PostgreSQL GUC registration.

use crate::settings;

#[cfg(feature = "pg")]
use std::ffi::CString;

#[cfg(feature = "pg")]
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

#[cfg(feature = "pg")]
static COLD_READS: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c"auto"));
#[cfg(feature = "pg")]
static MAX_OPEN_PARQUET_READERS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_MAX_OPEN_PARQUET_READERS);
#[cfg(feature = "pg")]
static MAX_RUNNING_JOBS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_MAX_RUNNING_JOBS);
#[cfg(feature = "pg")]
static LOG_LEVEL: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c"info"));
#[cfg(feature = "pg")]
static ENABLE_MERGE_SCAN: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pg")]
static INTERNAL_SYSTEM_WRITE: GucSetting<bool> = GucSetting::<bool>::new(false);
#[cfg(feature = "pg")]
static INTERNAL_FLUSH_CLEANUP: GucSetting<bool> = GucSetting::<bool>::new(false);
#[cfg(feature = "pg")]
static INTERNAL_ASYNC_MIRROR_WORKER: GucSetting<bool> = GucSetting::<bool>::new(true);
#[cfg(feature = "pg")]
static MIN_MAX_ROWS_PER_FILE: GucSetting<i32> =
    GucSetting::<i32>::new(settings::default_min_max_rows_per_file());
#[cfg(feature = "pg")]
static FAILPOINT: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c""));
#[cfg(feature = "pg")]
static PENDING_SEGMENT_TTL_SECONDS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_PENDING_SEGMENT_TTL_SECONDS);
#[cfg(feature = "pg")]
static FLUSH_CHECK_INTERVAL_SECONDS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_FLUSH_CHECK_INTERVAL_SECONDS);
#[cfg(feature = "pg")]
static ASYNC_APPLY_POLL_INTERVAL_MS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_ASYNC_APPLY_POLL_INTERVAL_MS);
#[cfg(feature = "pg")]
static ASYNC_APPLY_MAX_ROWS_PER_TICK: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_ASYNC_APPLY_MAX_ROWS_PER_TICK);
#[cfg(feature = "pg")]
static ASYNC_APPLY_MAX_MS_PER_TICK: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_ASYNC_APPLY_MAX_MS_PER_TICK);
#[cfg(feature = "pg")]
static FLUSH_PRELOCK_MAX_PASSES: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_FLUSH_PRELOCK_MAX_PASSES);
#[cfg(feature = "pg")]
static FLUSH_PRELOCK_MAX_MS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_FLUSH_PRELOCK_MAX_MS);
#[cfg(feature = "pg")]
static ASYNC_MIRROR_MAX_RETAINED_BYTES: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_ASYNC_MIRROR_MAX_RETAINED_BYTES);

/// Defines pg-koldstore configuration variables.
#[cfg(feature = "pg")]
pub fn define_gucs() {
    let flags = GucFlags::default();
    GucRegistry::define_string_guc(
        c"koldstore.cold_reads",
        c"Controls KoldStore cold reads.",
        c"Controls whether KoldStore reads cold Parquet data. Supported values are auto, on, and off.",
        &COLD_READS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.max_open_parquet_readers",
        c"Maximum open KoldStore Parquet readers.",
        c"Caps concurrent open Parquet readers per PostgreSQL backend (fail-fast when exceeded).",
        &MAX_OPEN_PARQUET_READERS,
        settings::MIN_CONCURRENCY_LIMIT,
        settings::MAX_CONCURRENCY_LIMIT,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.max_running_jobs",
        c"Maximum running KoldStore jobs.",
        c"Caps concurrently claimed KoldStore jobs.",
        &MAX_RUNNING_JOBS,
        settings::MIN_CONCURRENCY_LIMIT,
        settings::MAX_CONCURRENCY_LIMIT,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_string_guc(
        c"koldstore.log_level",
        c"KoldStore log level.",
        c"Controls KoldStore logging verbosity. Intended values are error, warn, info, debug, and trace.",
        &LOG_LEVEL,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.enable_merge_scan",
        c"Enables KoldStore merge scans.",
        c"Required for managed-table SELECT. When off, KoldMergeScan errors instead of allowing an incorrect heap-only read.",
        &ENABLE_MERGE_SCAN,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.internal_system_write",
        c"Allows internal KoldStore system writes.",
        c"Internal guard used by extension-owned maintenance paths.",
        &INTERNAL_SYSTEM_WRITE,
        GucContext::Suset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.internal_flush_cleanup",
        c"Allows internal KoldStore flush cleanup.",
        c"Internal guard used while pruning flushed hot and mirror rows.",
        &INTERNAL_FLUSH_CLEANUP,
        GucContext::Suset,
        flags,
    );
    GucRegistry::define_bool_guc(
        c"koldstore.internal_async_mirror_worker",
        c"Enables automatic async mirror worker registration.",
        c"Internal benchmark control. Keep enabled in production so async mirrors apply committed WAL automatically.",
        &INTERNAL_ASYNC_MIRROR_WORKER,
        GucContext::Suset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.min_max_rows_per_file",
        c"Minimum allowed max_rows_per_file for managed tables.",
        c"Rejects manage_table and flush settings below this floor. Lower temporarily for tests with SET koldstore.min_max_rows_per_file = <value>.",
        &MIN_MAX_ROWS_PER_FILE,
        settings::MIN_MIN_MAX_ROWS_PER_FILE,
        settings::MAX_MIN_MAX_ROWS_PER_FILE,
        GucContext::Userset,
        flags,
    );
    // Test-only: empty default keeps production paths inert unless explicitly armed.
    GucRegistry::define_string_guc(
        c"koldstore.failpoint",
        c"Test-only KoldStore flush failpoint.",
        c"Arms a named flush failpoint (error:<name> or wait:<name>). Empty disables. For crash-recovery and isolation suites only.",
        &FAILPOINT,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.pending_segment_ttl_seconds",
        c"TTL for pending cold segments before recovery expiry.",
        c"recover_segments quarantines object-store blobs and deletes catalog rows for pending segments older than this many seconds.",
        &PENDING_SEGMENT_TTL_SECONDS,
        settings::MIN_PENDING_SEGMENT_TTL_SECONDS,
        settings::MAX_PENDING_SEGMENT_TTL_SECONDS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.flush_check_interval_seconds",
        c"Interval between built-in auto-flush eligibility checks.",
        c"Database worker wakes on this cadence to evaluate auto_flush managed tables, enqueue flush jobs when needed, and run one flush. SET / ALTER SYSTEM + reload; workers pick up changes on SIGHUP.",
        &FLUSH_CHECK_INTERVAL_SECONDS,
        settings::MIN_FLUSH_CHECK_INTERVAL_SECONDS,
        settings::MAX_FLUSH_CHECK_INTERVAL_SECONDS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.async_apply_poll_interval_ms",
        c"Async mirror latch poll interval in milliseconds.",
        c"Database worker latch wait between apply wakes. Clamped to 50..=5000. Prefer ALTER DATABASE / ALTER SYSTEM + reload; session SET does not affect the bgworker. Workers pick up changes on SIGHUP.",
        &ASYNC_APPLY_POLL_INTERVAL_MS,
        settings::MIN_ASYNC_APPLY_POLL_INTERVAL_MS,
        settings::MAX_ASYNC_APPLY_POLL_INTERVAL_MS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.async_apply_max_rows_per_tick",
        c"Maximum source row changes applied in one async mirror tick.",
        c"Bounds work per apply transaction. 0 drains all peekable WAL in the tick (legacy). Explicit fences (wait_for_async_mirror / flush) may loop with a higher effective budget.",
        &ASYNC_APPLY_MAX_ROWS_PER_TICK,
        settings::MIN_ASYNC_APPLY_MAX_ROWS_PER_TICK,
        settings::MAX_ASYNC_APPLY_MAX_ROWS_PER_TICK,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.async_apply_max_ms_per_tick",
        c"Maximum wall-clock milliseconds for one async mirror tick.",
        c"Bounds apply transaction duration. 0 disables the time budget. Commit applied_lsn and continue on the next wake when the budget is exhausted.",
        &ASYNC_APPLY_MAX_MS_PER_TICK,
        settings::MIN_ASYNC_APPLY_MAX_MS_PER_TICK,
        settings::MAX_ASYNC_APPLY_MAX_MS_PER_TICK,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.flush_prelock_max_passes",
        c"Maximum phase-5.5 pre-lock async apply passes during flush.",
        c"Finite catch-up after Parquet upload and before SHARE ROW EXCLUSIVE. Fail closed when the budget is exhausted rather than holding writers indefinitely.",
        &FLUSH_PRELOCK_MAX_PASSES,
        settings::MIN_FLUSH_PRELOCK_MAX_PASSES,
        settings::MAX_FLUSH_PRELOCK_MAX_PASSES,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.flush_prelock_max_ms",
        c"Maximum wall-clock milliseconds for flush phase-5.5 pre-lock catch-up.",
        c"Combined budget across pre-lock passes. Exceeding this fails the flush before taking the source relation lock.",
        &FLUSH_PRELOCK_MAX_MS,
        settings::MIN_FLUSH_PRELOCK_MAX_MS,
        settings::MAX_FLUSH_PRELOCK_MAX_MS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.async_mirror_max_retained_bytes",
        c"Fail-closed admission limit on async mirror retained WAL bytes (default 1 GiB).",
        c"When > 0 and pg_wal_lsn_diff(current, confirmed_flush_lsn) exceeds this, apply and flush error instead of advancing. Default 1073741824 (1 GiB). 0 disables (not recommended for production async). Never silently drops WAL.",
        &ASYNC_MIRROR_MAX_RETAINED_BYTES,
        settings::MIN_ASYNC_MIRROR_MAX_RETAINED_BYTES,
        settings::MAX_ASYNC_MIRROR_MAX_RETAINED_BYTES,
        GucContext::Userset,
        flags,
    );
}

/// No-op placeholder for non-PostgreSQL tests.
#[cfg(not(feature = "pg"))]
pub fn define_gucs() {}

/// Static description of a pg-koldstore GUC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GucDefinition {
    /// GUC name.
    pub name: &'static str,
    /// Whether normal application roles are forbidden from setting it.
    pub internal: bool,
    /// Default value.
    pub default_value: &'static str,
}

/// Returns all GUC definitions.
#[must_use]
pub const fn definitions() -> &'static [GucDefinition] {
    &[
        GucDefinition {
            name: USER_ID_GUC,
            internal: false,
            default_value: "",
        },
        GucDefinition {
            name: ENABLE_MERGE_SCAN_GUC,
            internal: false,
            default_value: "on",
        },
        GucDefinition {
            name: settings::COLD_READS_GUC,
            internal: false,
            default_value: settings::DEFAULT_COLD_READS,
        },
        GucDefinition {
            name: settings::MAX_OPEN_PARQUET_READERS_GUC,
            internal: false,
            default_value: "32",
        },
        GucDefinition {
            name: settings::MAX_RUNNING_JOBS_GUC,
            internal: false,
            default_value: "4",
        },
        GucDefinition {
            name: settings::LOG_LEVEL_GUC,
            internal: false,
            default_value: settings::DEFAULT_LOG_LEVEL,
        },
        GucDefinition {
            name: settings::MIN_MAX_ROWS_PER_FILE_GUC,
            internal: false,
            default_value: "1000",
        },
        GucDefinition {
            name: INTERNAL_SYSTEM_WRITE_GUC,
            internal: true,
            default_value: "off",
        },
        GucDefinition {
            name: INTERNAL_FLUSH_CLEANUP_GUC,
            internal: true,
            default_value: "off",
        },
        GucDefinition {
            name: INTERNAL_ASYNC_MIRROR_WORKER_GUC,
            internal: true,
            default_value: "on",
        },
        GucDefinition {
            name: settings::FAILPOINT_GUC,
            internal: false,
            default_value: settings::DEFAULT_FAILPOINT,
        },
        GucDefinition {
            name: settings::PENDING_SEGMENT_TTL_SECONDS_GUC,
            internal: false,
            default_value: "3600",
        },
        GucDefinition {
            name: settings::FLUSH_CHECK_INTERVAL_SECONDS_GUC,
            internal: false,
            default_value: "30",
        },
        GucDefinition {
            name: settings::ASYNC_APPLY_POLL_INTERVAL_MS_GUC,
            internal: false,
            default_value: "100",
        },
        GucDefinition {
            name: settings::ASYNC_APPLY_MAX_ROWS_PER_TICK_GUC,
            internal: false,
            default_value: "0",
        },
        GucDefinition {
            name: settings::ASYNC_APPLY_MAX_MS_PER_TICK_GUC,
            internal: false,
            default_value: "0",
        },
        GucDefinition {
            name: settings::FLUSH_PRELOCK_MAX_PASSES_GUC,
            internal: false,
            default_value: "3",
        },
        GucDefinition {
            name: settings::FLUSH_PRELOCK_MAX_MS_GUC,
            internal: false,
            default_value: "5000",
        },
        GucDefinition {
            name: settings::ASYNC_MIRROR_MAX_RETAINED_BYTES_GUC,
            internal: false,
            default_value: "1073741824",
        },
    ]
}

/// Names of GUCs owned by pg-koldstore.
pub const USER_ID_GUC: &str = "koldstore.user_id";
pub const ENABLE_MERGE_SCAN_GUC: &str = "koldstore.enable_merge_scan";
pub const INTERNAL_SYSTEM_WRITE_GUC: &str = "koldstore.internal_system_write";
pub const INTERNAL_FLUSH_CLEANUP_GUC: &str = "koldstore.internal_flush_cleanup";
pub const INTERNAL_ASYNC_MIRROR_WORKER_GUC: &str = "koldstore.internal_async_mirror_worker";

/// Whether the planner may inject KoldMergeScan paths.
#[must_use]
pub fn enable_merge_scan() -> bool {
    #[cfg(feature = "pg")]
    {
        ENABLE_MERGE_SCAN.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        true
    }
}

/// Whether async capture should register the bounded-lag database worker.
///
/// This is disabled only by deterministic benchmarks that account for each
/// explicit catch-up phase. Production sessions keep the default enabled.
#[must_use]
pub fn async_mirror_worker_enabled() -> bool {
    #[cfg(feature = "pg")]
    {
        INTERNAL_ASYNC_MIRROR_WORKER.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        true
    }
}

/// Current cold-read mode.
#[must_use]
pub fn cold_reads_mode() -> settings::ColdReadsMode {
    #[cfg(feature = "pg")]
    {
        let value = COLD_READS
            .get()
            .and_then(|value| value.to_str().ok().map(str::to_string))
            .unwrap_or_else(|| settings::DEFAULT_COLD_READS.to_string());
        settings::ColdReadsMode::parse(&value).unwrap_or(settings::ColdReadsMode::Auto)
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::ColdReadsMode::Auto
    }
}

/// Current maximum open Parquet readers.
#[must_use]
pub fn max_open_parquet_readers() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_concurrency_limit(MAX_OPEN_PARQUET_READERS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_MAX_OPEN_PARQUET_READERS
    }
}

/// Current maximum running jobs.
#[must_use]
pub fn max_running_jobs() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_concurrency_limit(MAX_RUNNING_JOBS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_MAX_RUNNING_JOBS
    }
}

/// Current minimum allowed `max_rows_per_file` for managed tables.
#[must_use]
pub fn min_max_rows_per_file() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_min_max_rows_per_file(MIN_MAX_ROWS_PER_FILE.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::default_min_max_rows_per_file()
    }
}

/// Current test-only failpoint arming value (empty when disabled).
#[must_use]
pub fn failpoint_value() -> String {
    #[cfg(feature = "pg")]
    {
        FAILPOINT
            .get()
            .and_then(|value| value.to_str().ok().map(str::to_string))
            .unwrap_or_default()
    }

    #[cfg(not(feature = "pg"))]
    {
        String::new()
    }
}

/// TTL in seconds for pending cold segments before recover_segments expires them.
#[must_use]
pub fn pending_segment_ttl_seconds() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(
            PENDING_SEGMENT_TTL_SECONDS
                .get()
                .max(settings::MIN_PENDING_SEGMENT_TTL_SECONDS),
        )
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_PENDING_SEGMENT_TTL_SECONDS)
    }
}

/// Seconds between built-in auto-flush eligibility checks in the database worker.
#[must_use]
pub fn flush_check_interval_seconds() -> i64 {
    #[cfg(feature = "pg")]
    {
        let value = FLUSH_CHECK_INTERVAL_SECONDS.get();
        i64::from(value.clamp(
            settings::MIN_FLUSH_CHECK_INTERVAL_SECONDS,
            settings::MAX_FLUSH_CHECK_INTERVAL_SECONDS,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_FLUSH_CHECK_INTERVAL_SECONDS)
    }
}

/// Latch poll interval for the async mirror apply loop, in milliseconds.
#[must_use]
pub fn async_apply_poll_interval_ms() -> u64 {
    #[cfg(feature = "pg")]
    {
        let value = ASYNC_APPLY_POLL_INTERVAL_MS.get();
        u64::try_from(value.clamp(
            settings::MIN_ASYNC_APPLY_POLL_INTERVAL_MS,
            settings::MAX_ASYNC_APPLY_POLL_INTERVAL_MS,
        ))
        .unwrap_or(u64::from(
            settings::DEFAULT_ASYNC_APPLY_POLL_INTERVAL_MS as u32,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        u64::try_from(settings::DEFAULT_ASYNC_APPLY_POLL_INTERVAL_MS).unwrap_or(100)
    }
}

/// Maximum source row changes applied in one background apply tick (`0` = unlimited).
#[must_use]
pub fn async_apply_max_rows_per_tick() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(ASYNC_APPLY_MAX_ROWS_PER_TICK.get().clamp(
            settings::MIN_ASYNC_APPLY_MAX_ROWS_PER_TICK,
            settings::MAX_ASYNC_APPLY_MAX_ROWS_PER_TICK,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_ASYNC_APPLY_MAX_ROWS_PER_TICK)
    }
}

/// Maximum wall-clock milliseconds for one background apply tick (`0` = unlimited).
#[must_use]
pub fn async_apply_max_ms_per_tick() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(ASYNC_APPLY_MAX_MS_PER_TICK.get().clamp(
            settings::MIN_ASYNC_APPLY_MAX_MS_PER_TICK,
            settings::MAX_ASYNC_APPLY_MAX_MS_PER_TICK,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_ASYNC_APPLY_MAX_MS_PER_TICK)
    }
}

/// Maximum phase-5.5 pre-lock apply passes during flush.
#[must_use]
pub fn flush_prelock_max_passes() -> i32 {
    #[cfg(feature = "pg")]
    {
        FLUSH_PRELOCK_MAX_PASSES.get().clamp(
            settings::MIN_FLUSH_PRELOCK_MAX_PASSES,
            settings::MAX_FLUSH_PRELOCK_MAX_PASSES,
        )
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_FLUSH_PRELOCK_MAX_PASSES
    }
}

/// Combined wall-clock budget (ms) for flush phase-5.5 pre-lock catch-up.
#[must_use]
pub fn flush_prelock_max_ms() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(FLUSH_PRELOCK_MAX_MS.get().clamp(
            settings::MIN_FLUSH_PRELOCK_MAX_MS,
            settings::MAX_FLUSH_PRELOCK_MAX_MS,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_FLUSH_PRELOCK_MAX_MS)
    }
}

/// Retained-WAL admission limit in bytes (`0` = disabled).
#[must_use]
pub fn async_mirror_max_retained_bytes() -> i64 {
    #[cfg(feature = "pg")]
    {
        i64::from(ASYNC_MIRROR_MAX_RETAINED_BYTES.get().clamp(
            settings::MIN_ASYNC_MIRROR_MAX_RETAINED_BYTES,
            settings::MAX_ASYNC_MIRROR_MAX_RETAINED_BYTES,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        i64::from(settings::DEFAULT_ASYNC_MIRROR_MAX_RETAINED_BYTES)
    }
}
