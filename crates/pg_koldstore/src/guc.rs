//! PostgreSQL GUC registration.

use crate::settings;

#[cfg(feature = "pg")]
use std::ffi::CString;

#[cfg(feature = "pg")]
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

#[cfg(feature = "pg")]
static COLD_READS: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(Some(c"auto"));
#[cfg(feature = "pg")]
static USER_ID: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
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
static MIN_MAX_ROWS_PER_FILE: GucSetting<i32> =
    GucSetting::<i32>::new(settings::default_min_max_rows_per_file());

/// Defines pg-koldstore configuration variables.
#[cfg(feature = "pg")]
pub fn define_gucs() {
    let flags = GucFlags::default();
    GucRegistry::define_string_guc(
        c"koldstore.user_id",
        c"Active KoldStore user-scope id.",
        c"Required for reads and writes on user-scoped managed tables. Empty means unset.",
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
    ]
}

/// Names of GUCs owned by pg-koldstore.
pub const USER_ID_GUC: &str = "koldstore.user_id";
pub const ENABLE_MERGE_SCAN_GUC: &str = "koldstore.enable_merge_scan";
pub const INTERNAL_SYSTEM_WRITE_GUC: &str = "koldstore.internal_system_write";
pub const INTERNAL_FLUSH_CLEANUP_GUC: &str = "koldstore.internal_flush_cleanup";

/// Active `koldstore.user_id` value, or `None` when unset/blank.
#[must_use]
pub fn user_id() -> Option<String> {
    #[cfg(feature = "pg")]
    {
        let value = USER_ID
            .get()
            .and_then(|value| value.to_str().ok().map(str::to_string));
        koldstore_common::normalize_user_id(value.as_deref())
    }

    #[cfg(not(feature = "pg"))]
    {
        None
    }
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
