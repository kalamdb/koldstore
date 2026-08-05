//! SPI/GUC adapter for flush failpoints.
//!
//! Typed names and action-prefix parsing live in [`koldstore_flush::failpoints`].
//! This module arms via GUC `koldstore.failpoint` and runs wait barriers over SPI.
//!
//! Supported GUC values:
//!
//! - `<name>` or `error:<name>` — abort with an error at that phase
//! - `wait:<name>` — block on the advisory barrier lock until another session unlocks
//!
//! Prefer [`hit_typed`] with [`FlushFailpoint`] at call sites. [`hit`] remains for
//! string-compatible callers (mirror apply constants, older tests).

pub use koldstore_flush::{FailpointAction, FlushFailpoint, FAILPOINT_NAMES};

/// Advisory lock key shared with E2E isolation/crash harnesses (`"KOLD"`).
pub const FAILPOINT_BARRIER_KEY: i64 = 0x4B4F_4C44;

/// Hits a named failpoint if the session GUC arms it.
///
/// # Errors
///
/// Returns an error when the failpoint is armed for abort, or when the wait
/// barrier / SPI call fails.
pub fn hit(name: &str) -> Result<(), String> {
    let armed = current_failpoint();
    if armed.is_empty() {
        return Ok(());
    }

    let (action, target) = FailpointAction::parse_prefix(&armed);
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
        FailpointAction::Wait => wait_barrier(name),
        #[cfg(feature = "test-failpoints")]
        FailpointAction::Panic => {
            // Stub: SIGKILL / panic harness lands with the process-kill work.
            Err(format!(
                "koldstore failpoint panic stub (not implemented): {name}"
            ))
        }
        #[cfg(feature = "test-failpoints")]
        FailpointAction::Sleep => {
            // Stub: timed sleep harness lands with destructive action wiring.
            Err(format!(
                "koldstore failpoint sleep stub (not implemented): {name}"
            ))
        }
        #[cfg(not(feature = "test-failpoints"))]
        FailpointAction::Panic | FailpointAction::Sleep => {
            // Production builds ignore destructive prefixes; treat as error abort.
            Err(format!("koldstore failpoint hit: {name}"))
        }
    }
}

fn current_failpoint() -> String {
    #[cfg(feature = "pg")]
    {
        crate::guc::failpoint_value()
    }
    #[cfg(not(feature = "pg"))]
    {
        String::new()
    }
}

fn wait_barrier(name: &str) -> Result<(), String> {
    #[cfg(feature = "pg")]
    {
        use pgrx::datum::DatumWithOid;
        // Block until the coordinating session releases the barrier lock.
        // pg_advisory_lock/unlock return void — use Spi::run, not bool decode.
        pgrx::Spi::run_with_args(
            "SELECT pg_advisory_lock($1)",
            &[DatumWithOid::from(FAILPOINT_BARRIER_KEY)],
        )
        .map_err(|error| error.to_string())?;
        pgrx::Spi::run_with_args(
            "SELECT pg_advisory_unlock($1)",
            &[DatumWithOid::from(FAILPOINT_BARRIER_KEY)],
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
