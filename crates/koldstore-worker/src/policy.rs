//! Poll and library naming policy for database-scoped workers.
//!
//! Appliers use `BGW_NEVER_RESTART` so intentional slot drop leaves them stopped.
//! A cluster launcher (auto-restarted) and the first backend query re-register
//! appliers after crashes or postmaster restart. No per-DML kick triggers.

/// Shared library name loaded by dynamic background workers.
pub const LIBRARY_NAME: &str = "koldstore";

/// Default latch poll interval for the async mirror apply loop, in milliseconds.
///
/// Bounds mirror lag without spinning; each tick only peeks when WAL advanced
/// past the slot's `confirmed_flush`. Runtime value is
/// `koldstore.async_apply_poll_interval_ms` (default 100).
pub const APPLY_POLL_INTERVAL_MS: u64 = 100;

/// Maximum idle latch backoff after consecutive empty peeks, in milliseconds.
///
/// Caps how long the applier sleeps when cluster WAL advances without
/// publication changes, so empty decode work cannot pin a core.
pub const APPLY_IDLE_BACKOFF_MAX_MS: u64 = 5_000;

/// Maximum budget-exhausted apply ticks retried before yielding to the latch.
///
/// This lets bounded catch-up avoid a full poll delay between every chunk while
/// ensuring foreground backends and scheduled flush work receive regular CPU.
pub const MAX_IMMEDIATE_PENDING_TICKS: u8 = 4;

/// Launcher poll interval while discovering databases that need an applier.
///
/// Kept in seconds-scale range: ensure is cheap when the oid set is unchanged,
/// and NEVER_RESTART appliers only need re-registration after crashes.
pub const LAUNCHER_POLL_INTERVAL_MS: u64 = 2_000;
