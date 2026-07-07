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
