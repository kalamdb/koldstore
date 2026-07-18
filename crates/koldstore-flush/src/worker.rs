//! Background flush scheduling mode notes.
//!
//! Built-in scheduling runs on the shared database worker in `pg_koldstore`.
//! Without preload, operators use SQL (`flush_table`) or pg_cron. See
//! `docs/operations/scheduling.md`.

/// Returns whether built-in worker registration requires shared preload for
/// postmaster-restart recovery via the cluster launcher.
#[must_use]
pub const fn requires_shared_preload() -> bool {
    true
}
