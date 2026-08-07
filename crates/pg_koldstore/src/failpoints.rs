//! SPI/GUC adapter for flush failpoints.
//!
//! Typed names and action-prefix parsing live in [`koldstore_flush::failpoints`].
//! This module arms via GUC `koldstore.failpoint` and runs wait barriers over SPI.
//!
//! Supported GUC values:
//!
//! - `<name>` or `error:<name>` — abort with an error at that phase
//! - `wait:<name>` — block on the advisory barrier until another session unlocks
//!   (**requires `test-failpoints`**; production builds treat this as an error so
//!   a mistaken `SET` cannot park a backend forever)
//! - `panic:<name>` — hard-abort the backend (`std::process::abort`; SIGKILL-equivalent
//!   for harnesses; requires `test-failpoints`)
//! - `sleep:<name>` — sleep ~5s then continue (requires `test-failpoints`)
//!
//! Prefer [`hit_typed`] with [`FlushFailpoint`] at call sites. [`hit`] remains for
//! string-compatible callers (mirror apply constants, older tests).

pub use koldstore_flush::{FailpointAction, FlushFailpoint, FAILPOINT_NAMES};

/// Advisory-lock namespace for `wait:` failpoints (`"KOLD"` as i32).
///
/// Paired with [`pgrx::pg_sys::MyDatabaseId`] so parallel E2E worker databases
/// do not share one cluster-wide barrier (bigint `pg_advisory_lock` keyed only
/// on `"KOLD"` previously let one test steal another's unlock).
pub const FAILPOINT_BARRIER_NAMESPACE: i32 = 0x4B4F_4C44;

/// Fixed sleep duration for `sleep:` failpoints (v1; optional duration parse later).
#[cfg(feature = "test-failpoints")]
const FAILPOINT_SLEEP: std::time::Duration = std::time::Duration::from_secs(5);

/// Hits a named failpoint if the session GUC arms it.
///
/// Borrows the armed GUC text in place (no owned-`String` conversion) so the
/// disarmed fast path — the overwhelming majority of calls in production —
/// costs no more than the unavoidable GUC read.
///
/// # Errors
///
/// Returns an error when the failpoint is armed for abort, or when the wait
/// barrier / SPI call fails.
pub fn hit(name: &str) -> Result<(), String> {
    let Some(armed) = current_failpoint() else {
        return Ok(());
    };
    let Ok(armed) = armed.to_str() else {
        return Ok(());
    };
    if armed.is_empty() {
        return Ok(());
    }

    let (action, target) = FailpointAction::parse_prefix(armed);
    if target != name {
        return Ok(());
    }

    dispatch_action(action, name)
}

/// Hits a typed failpoint if the session GUC arms it.
///
/// # Errors
///
/// Returns an error when the failpoint is armed for abort, or when the wait
/// barrier / SPI call fails.
pub fn hit_typed(point: FlushFailpoint) -> Result<(), String> {
    hit(point.as_str())
}

fn dispatch_action(action: FailpointAction, name: &str) -> Result<(), String> {
    match action {
        FailpointAction::Error => Err(format!("koldstore failpoint hit: {name}")),
        #[cfg(feature = "test-failpoints")]
        FailpointAction::Wait => wait_barrier(name),
        #[cfg(feature = "test-failpoints")]
        FailpointAction::Panic => {
            // Immediate hard exit — documents SIGKILL-equivalent for process-kill
            // harnesses. Prefer external SIGKILL from the test process in e2e.
            pgrx::log!("koldstore failpoint panic: aborting process at {name}");
            std::process::abort();
        }
        #[cfg(feature = "test-failpoints")]
        FailpointAction::Sleep => {
            pgrx::log!("koldstore failpoint sleep: {name} for {FAILPOINT_SLEEP:?}");
            std::thread::sleep(FAILPOINT_SLEEP);
            Ok(())
        }
        #[cfg(not(feature = "test-failpoints"))]
        FailpointAction::Wait | FailpointAction::Panic | FailpointAction::Sleep => {
            // Production builds refuse parking / destructive prefixes so a
            // mistaken SET cannot pin a flush executor forever.
            Err(format!(
                "koldstore failpoint '{name}' requires the test-failpoints build \
                 (wait/panic/sleep are disabled in production installs)"
            ))
        }
    }
}

fn current_failpoint() -> Option<std::ffi::CString> {
    #[cfg(feature = "pg")]
    {
        crate::guc::failpoint_value()
    }
    #[cfg(not(feature = "pg"))]
    {
        None
    }
}

/// Parks until the coordinator releases the barrier.
///
/// Uses a genuine blocking `pg_advisory_lock`, not a `pg_try_advisory_lock`
/// poll loop: PostgreSQL's own lock-wait machinery (`ProcSleep`) already
/// honors `pg_cancel_backend` / statement timeout / postmaster death while
/// parked, so no manual interrupt-check loop is needed. Just as important,
/// only a genuine blocking wait registers as a `granted = false` row in
/// `pg_locks` — the signal E2E harnesses poll for to detect "flush is
/// parked at the barrier". A `pg_try_advisory_lock` loop never blocks long
/// enough to be observed there, so harnesses would see the flush as never
/// having reached the wait and time out instead.
#[cfg(feature = "test-failpoints")]
fn wait_barrier(name: &str) -> Result<(), String> {
    #[cfg(feature = "pg")]
    {
        use pgrx::datum::DatumWithOid;
        // Per-database two-key lock: parallel E2E worker DBs must not share one
        // cluster-wide barrier. pg_advisory_lock/unlock return void — use
        // Spi::run, not bool decode.
        let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32() as i32;
        pgrx::Spi::run_with_args(
            "SELECT pg_advisory_lock($1, $2)",
            &[
                DatumWithOid::from(FAILPOINT_BARRIER_NAMESPACE),
                DatumWithOid::from(database_oid),
            ],
        )
        .map_err(|error| error.to_string())?;
        pgrx::Spi::run_with_args(
            "SELECT pg_advisory_unlock($1, $2)",
            &[
                DatumWithOid::from(FAILPOINT_BARRIER_NAMESPACE),
                DatumWithOid::from(database_oid),
            ],
        )
        .map_err(|error| error.to_string())?;
        pgrx::log!("koldstore failpoint wait released: {name}");
        Ok(())
    }
    #[cfg(not(feature = "pg"))]
    {
        let _ = name;
        Ok(())
    }
}
