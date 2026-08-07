//! PostgreSQL GUC registration.

use crate::settings;

#[cfg(feature = "pg")]
use std::ffi::CString;

#[cfg(feature = "pg")]
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

#[cfg(feature = "pg")]
static COLD_READS: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c"auto"));
#[cfg(feature = "pg")]
static USER_ID: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c""));
#[cfg(feature = "pg")]
static MAX_OPEN_PARQUET_READERS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_MAX_OPEN_PARQUET_READERS);
#[cfg(feature = "pg")]
static MAX_MERGE_SEEN_KEYS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_MAX_MERGE_SEEN_KEYS);
#[cfg(feature = "pg")]
static OBJECT_STORE_TIMEOUT_MS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_OBJECT_STORE_TIMEOUT_MS);
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
static MAX_PARALLEL_FLUSH_JOBS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_MAX_PARALLEL_FLUSH_JOBS);
#[cfg(feature = "pg")]
static FLUSH_JOB_MAX_RUNTIME_SECONDS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_FLUSH_JOB_MAX_RUNTIME_SECONDS);
#[cfg(feature = "pg")]
static JOB_RETENTION_DAYS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_JOB_RETENTION_DAYS);
#[cfg(feature = "pg")]
static FLUSH_EXECUTION: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"queue"));
#[cfg(feature = "pg")]
static ASYNC_APPLY_WATCHDOG_INTERVAL_MS: GucSetting<i32> =
    GucSetting::<i32>::new(settings::DEFAULT_ASYNC_APPLY_WATCHDOG_INTERVAL_MS);
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
        c"koldstore.user_id",
        c"Active user/tenant scope for user-scoped managed tables.",
        c"Fail-closed session scope for user-typed tables. Must be set before scoped DML, SELECT, and changes_since. Empty means unset.",
        &USER_ID,
        GucContext::Userset,
        flags,
    );
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
        c"koldstore.max_merge_seen_keys",
        c"Maximum exact PK identities retained by one KoldMergeScan.",
        c"Fail-closed per-scan cap on the compact winner seen-set. Protects backends from accidental full-table scans. 0 disables the cap.",
        &MAX_MERGE_SEEN_KEYS,
        settings::MIN_MAX_MERGE_SEEN_KEYS,
        settings::MAX_MAX_MERGE_SEEN_KEYS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.object_store_timeout_ms",
        c"Timeout for one ObjectStore or Parquet segment operation.",
        c"Fail-fast wall-clock budget for cold reads and flush object I/O. 0 disables the timeout (query cancel still aborts in-flight waits). Clamped to 0..=600000 milliseconds.",
        &OBJECT_STORE_TIMEOUT_MS,
        settings::MIN_OBJECT_STORE_TIMEOUT_MS,
        settings::MAX_OBJECT_STORE_TIMEOUT_MS,
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
        c"Rejects manage_table (and ALTER) settings below this floor. Already-persisted catalog policies are trusted at flush time so queue executors need not inherit the managing session's SET. Lower temporarily for tests with SET / ALTER DATABASE koldstore.min_max_rows_per_file = <value>.",
        &MIN_MAX_ROWS_PER_FILE,
        settings::MIN_MIN_MAX_ROWS_PER_FILE,
        settings::MAX_MIN_MAX_ROWS_PER_FILE,
        GucContext::Userset,
        flags,
    );
    // Test-only: empty default keeps production paths inert unless explicitly armed.
    // wait/panic/sleep require the test-failpoints build so production cannot park.
    GucRegistry::define_string_guc(
        c"koldstore.failpoint",
        c"Test-only KoldStore flush failpoint.",
        c"Arms a named flush failpoint (error:<name>, wait:<name>, panic:<name>, or sleep:<name>). Empty disables. wait/panic/sleep require a test-failpoints build. For crash-recovery and isolation suites only.",
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
        c"Database worker wakes on this cadence to evaluate auto_flush managed tables, enqueue flush jobs when needed, and spawn flush executors. SET / ALTER SYSTEM + reload; workers pick up changes on SIGHUP.",
        &FLUSH_CHECK_INTERVAL_SECONDS,
        settings::MIN_FLUSH_CHECK_INTERVAL_SECONDS,
        settings::MAX_FLUSH_CHECK_INTERVAL_SECONDS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.max_parallel_flush_jobs",
        c"Maximum concurrent one-shot flush executor workers per database.",
        c"Caps how many koldstore flush executor background workers may run at once. Default stays at 2 until broader failure-sweep coverage lands. Clamped to 1..=16.",
        &MAX_PARALLEL_FLUSH_JOBS,
        settings::MIN_MAX_PARALLEL_FLUSH_JOBS,
        settings::MAX_MAX_PARALLEL_FLUSH_JOBS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.flush_job_max_runtime_seconds",
        c"Wall-clock budget for one flush job attempt.",
        c"Flush aborts with an error when a single attempt exceeds this many seconds (checked between passes and between streamed batches within a pass). 0 disables. Default 1800 (30 minutes). Clamped to 0..=86400.",
        &FLUSH_JOB_MAX_RUNTIME_SECONDS,
        settings::MIN_FLUSH_JOB_MAX_RUNTIME_SECONDS,
        settings::MAX_FLUSH_JOB_MAX_RUNTIME_SECONDS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.job_retention_days",
        c"Days to retain terminal KoldStore jobs before purge.",
        c"Coordinator ticks delete completed/cancelled/error jobs whose finished_at is older than this many days. 0 disables purge. Jobs still referenced by pending cold segments are never deleted. Clamped to 0..=3650.",
        &JOB_RETENTION_DAYS,
        settings::MIN_JOB_RETENTION_DAYS,
        settings::MAX_JOB_RETENTION_DAYS,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_string_guc(
        c"koldstore.flush_execution",
        c"How flush_table runs after enqueueing a durable job.",
        c"queue (default): enqueue UUID and spawn a one-shot flush executor. inline: enqueue then run flush in the calling backend (required for pg_test SPI transactions).",
        &FLUSH_EXECUTION,
        GucContext::Userset,
        flags,
    );
    GucRegistry::define_int_guc(
        c"koldstore.async_apply_watchdog_interval_ms",
        c"Safety watchdog for commit-driven async mirror wakeups.",
        c"Managed commits normally wake the database worker immediately. This timeout recovers missed notifications without periodic short-interval decoding. Clamped to 1000..=300000 milliseconds.",
        &ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
        settings::MIN_ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
        settings::MAX_ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
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
        c"Unhealthy threshold for async mirror retained WAL bytes (default 1 GiB).",
        c"When > 0 and pg_wal_lsn_diff(current, confirmed_flush_lsn) exceeds this, async_mirror_status reports unhealthy. Apply always remains enabled so it can drain the slot. Default 1073741824 (1 GiB). 0 disables this health threshold. Never silently drops WAL.",
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
            name: settings::MAX_MERGE_SEEN_KEYS_GUC,
            internal: false,
            default_value: "1000000",
        },
        GucDefinition {
            name: settings::OBJECT_STORE_TIMEOUT_MS_GUC,
            internal: false,
            default_value: "30000",
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
            name: settings::MAX_PARALLEL_FLUSH_JOBS_GUC,
            internal: false,
            default_value: "2",
        },
        GucDefinition {
            name: settings::FLUSH_JOB_MAX_RUNTIME_SECONDS_GUC,
            internal: false,
            default_value: "1800",
        },
        GucDefinition {
            name: settings::JOB_RETENTION_DAYS_GUC,
            internal: false,
            default_value: "30",
        },
        GucDefinition {
            name: settings::FLUSH_EXECUTION_GUC,
            internal: false,
            default_value: settings::DEFAULT_FLUSH_EXECUTION,
        },
        GucDefinition {
            name: settings::ASYNC_APPLY_WATCHDOG_INTERVAL_MS_GUC,
            internal: false,
            default_value: "30000",
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

/// Active `koldstore.user_id` when set to a non-empty value.
#[must_use]
pub fn user_id() -> Option<String> {
    #[cfg(feature = "pg")]
    {
        let from_setting = USER_ID.get().and_then(|value| {
            value
                .to_str()
                .ok()
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(str::to_string)
        });
        from_setting.or_else(read_user_id_config_option)
    }

    #[cfg(not(feature = "pg"))]
    {
        None
    }
}

/// Fallback for placeholder GUCs set before the extension registered the setting.
#[cfg(feature = "pg")]
fn read_user_id_config_option() -> Option<String> {
    let setting = unsafe {
        let name = c"koldstore.user_id";
        let value = pgrx::pg_sys::GetConfigOption(name.as_ptr(), true, false);
        if value.is_null() {
            return None;
        }
        std::ffi::CStr::from_ptr(value)
            .to_string_lossy()
            .into_owned()
    };
    let trimmed = setting.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

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

/// Current per-scan merge seen-key cap (`0` = unlimited).
#[must_use]
pub fn max_merge_seen_keys() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_max_merge_seen_keys(MAX_MERGE_SEEN_KEYS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_MAX_MERGE_SEEN_KEYS
    }
}

/// ObjectStore / Parquet operation timeout (`None` when disabled / `0`).
#[must_use]
pub fn object_store_timeout() -> Option<std::time::Duration> {
    let ms = object_store_timeout_ms();
    (ms > 0).then(|| std::time::Duration::from_millis(ms))
}

/// ObjectStore / Parquet operation timeout in milliseconds (`0` = disabled).
#[must_use]
pub fn object_store_timeout_ms() -> u64 {
    #[cfg(feature = "pg")]
    {
        u64::try_from(settings::bounded_object_store_timeout_ms(
            OBJECT_STORE_TIMEOUT_MS.get(),
        ))
        .unwrap_or(0)
    }

    #[cfg(not(feature = "pg"))]
    {
        u64::try_from(settings::DEFAULT_OBJECT_STORE_TIMEOUT_MS).unwrap_or(30_000)
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

/// Current test-only failpoint arming value, owned but unparsed.
///
/// Returns the raw `CString` rather than an allocated `String` so the hot
/// `failpoints::hit` call sites (invoked at every flush phase and mirror
/// apply tick) can borrow the text with zero extra allocations instead of
/// paying for a fresh UTF-8 copy on every disarmed check.
#[must_use]
pub fn failpoint_value() -> Option<std::ffi::CString> {
    #[cfg(feature = "pg")]
    {
        FAILPOINT.get()
    }

    #[cfg(not(feature = "pg"))]
    {
        None
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

/// Maximum concurrent one-shot flush executor workers for this database.
#[must_use]
pub fn max_parallel_flush_jobs() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_max_parallel_flush_jobs(MAX_PARALLEL_FLUSH_JOBS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_MAX_PARALLEL_FLUSH_JOBS
    }
}

/// Wall-clock budget for one flush job attempt (`0` = disabled).
#[must_use]
pub fn flush_job_max_runtime_seconds() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_flush_job_max_runtime_seconds(FLUSH_JOB_MAX_RUNTIME_SECONDS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_FLUSH_JOB_MAX_RUNTIME_SECONDS
    }
}

/// Days to retain terminal jobs before coordinator purge (`0` = disabled).
#[must_use]
pub fn job_retention_days() -> i32 {
    #[cfg(feature = "pg")]
    {
        settings::bounded_job_retention_days(JOB_RETENTION_DAYS.get())
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::DEFAULT_JOB_RETENTION_DAYS
    }
}

/// Whether `flush_table` should run inline or enqueue for background executors.
#[must_use]
pub fn flush_execution_mode() -> settings::FlushExecutionMode {
    #[cfg(feature = "pg")]
    {
        let value = FLUSH_EXECUTION
            .get()
            .and_then(|value| value.to_str().ok().map(str::to_string))
            .unwrap_or_else(|| settings::DEFAULT_FLUSH_EXECUTION.to_string());
        settings::FlushExecutionMode::parse(&value).unwrap_or(settings::FlushExecutionMode::Queue)
    }

    #[cfg(not(feature = "pg"))]
    {
        settings::FlushExecutionMode::Queue
    }
}

/// Safety watchdog interval for commit-driven async mirror wakeups.
#[must_use]
pub fn async_apply_watchdog_interval_ms() -> u64 {
    #[cfg(feature = "pg")]
    {
        let value = ASYNC_APPLY_WATCHDOG_INTERVAL_MS.get();
        u64::try_from(value.clamp(
            settings::MIN_ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
            settings::MAX_ASYNC_APPLY_WATCHDOG_INTERVAL_MS,
        ))
        .unwrap_or(u64::from(
            settings::DEFAULT_ASYNC_APPLY_WATCHDOG_INTERVAL_MS as u32,
        ))
    }

    #[cfg(not(feature = "pg"))]
    {
        u64::try_from(settings::DEFAULT_ASYNC_APPLY_WATCHDOG_INTERVAL_MS).unwrap_or(30_000)
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

/// Retained-WAL unhealthy threshold in bytes (`0` = disabled).
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
