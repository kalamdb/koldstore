//! PostgreSQL advisory locks for table-scoped job execution.
//!
//! The durable jobs catalog prevents duplicate active rows. These transaction
//! locks add an early, low-cost guard around inline SQL entrypoints before they
//! perform setup work.
//!
//! Keys use the single-argument `bigint` advisory-lock form so every table OID
//! maps 1:1. The two-`integer` form forced a signed `i32` cast that obscures
//! high OIDs and is easy to get wrong at SPI boundaries.

/// Namespace for table-scoped flush/migration job locks (fits in 32 bits).
const TABLE_JOB_LOCK_NAMESPACE: i64 = 0x4b54_4a42;

/// Packs namespace + table OID into one PostgreSQL bigint advisory-lock key.
#[must_use]
pub(crate) const fn table_job_advisory_lock_key(table_oid: u32) -> i64 {
    (TABLE_JOB_LOCK_NAMESPACE << 32) | (table_oid as i64)
}

/// Takes a transaction-scoped lock for flush/migration work on one table.
///
/// Blocks until the lock is available so concurrent `flush_table` /
/// `manage_table` callers serialize rather than failing the loser with
/// try-lock contention.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot evaluate the advisory lock query.
pub fn lock_table_job(table_oid: pgrx::pg_sys::Oid) -> Result<(), String> {
    let key = table_job_advisory_lock_key(table_oid.to_u32());
    pgrx::Spi::run_with_args(
        "SELECT pg_advisory_xact_lock($1::bigint)",
        &[pgrx::datum::DatumWithOid::from(key)],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Attempts a non-blocking table job lock.
///
/// Returns `true` when this transaction now holds the lock (including when the
/// same backend already held it). Returns `false` when another backend is
/// mid-flush/migration so the caller can skip instead of waiting.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot evaluate the advisory lock query.
pub fn try_lock_table_job(table_oid: pgrx::pg_sys::Oid) -> Result<bool, String> {
    let key = table_job_advisory_lock_key(table_oid.to_u32());
    let acquired = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT pg_try_advisory_xact_lock($1::bigint)",
        &[pgrx::datum::DatumWithOid::from(key)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    Ok(acquired)
}

#[cfg(test)]
mod tests {
    use super::table_job_advisory_lock_key;

    #[test]
    fn high_oids_stay_distinct_from_low_oids() {
        let low = table_job_advisory_lock_key(1);
        let high = table_job_advisory_lock_key(u32::MAX);
        let mid = table_job_advisory_lock_key(i32::MAX as u32 + 1);
        assert_ne!(low, high);
        assert_ne!(low, mid);
        assert_ne!(high, mid);
        // OID bits occupy the low 32 bits without sign-wrapping.
        assert_eq!(low & 0xffff_ffff, 1);
        assert_eq!(high & 0xffff_ffff, u32::MAX as i64);
        assert_eq!(mid & 0xffff_ffff, (i32::MAX as u32 + 1) as i64);
    }
}
