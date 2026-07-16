//! Test-only failpoints for crash-recovery and isolation suites.
//!
//! Armed via GUC `koldstore.failpoint` (empty default). Release builds are inert
//! unless an operator explicitly sets the GUC. Supported values:
//!
//! - `<name>` or `error:<name>` — abort with an error at that phase
//! - `wait:<name>` — block on the advisory barrier lock until another session unlocks
//!
//! Failpoint names include the twelve flush crash points used by E2E recovery
//! tests plus `async_mirror_apply` for async WAL applier crash injection.

/// Advisory lock key shared with E2E isolation/crash harnesses (`"KOLD"`).
pub const FAILPOINT_BARRIER_KEY: i64 = 0x4B4F_4C44;

/// Canonical failpoint names (flush crash points + async apply).
pub const FAILPOINT_NAMES: &[&str] = &[
    "after_claim",
    "after_select_rows",
    "after_pending_segment",
    "during_parquet_write",
    "after_temp_object",
    "after_checksum_metadata",
    "before_manifest_publish",
    "after_manifest_publish",
    "before_hot_cleanup",
    "during_hot_cleanup",
    "after_cleanup_before_job_complete",
    "after_job_complete_before_temp_cleanup",
    "async_mirror_apply",
];

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

    let (mode, target) = parse_armed(&armed);
    if target != name {
        return Ok(());
    }

    match mode {
        FailMode::Error => Err(format!("koldstore failpoint hit: {name}")),
        FailMode::Wait => wait_barrier(name),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailMode {
    Error,
    Wait,
}

fn parse_armed(armed: &str) -> (FailMode, &str) {
    if let Some(rest) = armed.strip_prefix("wait:") {
        (FailMode::Wait, rest)
    } else if let Some(rest) = armed.strip_prefix("error:") {
        (FailMode::Error, rest)
    } else {
        (FailMode::Error, armed)
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
        let _ = pgrx::Spi::get_one_with_args::<bool>(
            "SELECT pg_advisory_lock($1)",
            &[DatumWithOid::from(FAILPOINT_BARRIER_KEY)],
        )
        .map_err(|error| error.to_string())?;
        let _ = pgrx::Spi::get_one_with_args::<bool>(
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
