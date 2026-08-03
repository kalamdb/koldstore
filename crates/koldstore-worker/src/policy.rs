//! Library naming and fairness policy for database-scoped workers.
//!
//! Appliers use `BGW_NEVER_RESTART` so intentional slot drop leaves them stopped.
//! A cluster launcher (auto-restarted) and the first backend query re-register
//! appliers after crashes or postmaster restart. Managed commits wake their
//! database worker through a coalescing shared generation.

/// Shared library name loaded by dynamic background workers.
pub const LIBRARY_NAME: &str = "koldstore";

/// Maximum budget-exhausted apply ticks retried before yielding to the latch.
///
/// This lets bounded catch-up avoid a full latch wait between every chunk while
/// ensuring foreground backends and scheduled flush work receive regular CPU.
pub const MAX_IMMEDIATE_PENDING_TICKS: u8 = 4;

/// Launcher poll interval while discovering databases that need an applier.
///
/// Kept in seconds-scale range: ensure is cheap when the oid set is unchanged,
/// and NEVER_RESTART appliers only need re-registration after crashes.
pub const LAUNCHER_POLL_INTERVAL_MS: u64 = 2_000;
