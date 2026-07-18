//! Poll and library naming policy for database-scoped workers.
//!
//! Appliers use `BGW_NEVER_RESTART` so intentional slot drop leaves them stopped.
//! A cluster launcher (auto-restarted) and the first backend query re-register
//! appliers after crashes or postmaster restart. No per-DML kick triggers.

/// Shared library name loaded by dynamic background workers.
pub const LIBRARY_NAME: &str = "koldstore";

/// Default latch poll interval for the async mirror apply loop, in milliseconds.
///
/// Bounds mirror lag without spinning; each tick only peeks when WAL advanced.
/// Runtime value is `koldstore.async_apply_poll_interval_ms` (default 100).
pub const APPLY_POLL_INTERVAL_MS: u64 = 100;

/// Launcher poll interval while discovering databases that need an applier.
pub const LAUNCHER_POLL_INTERVAL_MS: u64 = 250;
