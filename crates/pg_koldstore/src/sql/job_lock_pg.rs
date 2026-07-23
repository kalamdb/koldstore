//! PostgreSQL advisory locks for table-scoped job execution.
//!
//! The durable jobs catalog prevents duplicate active rows. These transaction
//! locks add an early, low-cost guard around inline SQL entrypoints before they
//! perform setup work.

const TABLE_JOB_LOCK_NAMESPACE: i32 = 0x4b54_4a42;

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
    let table_key = table_oid.to_u32() as i32;
    let args = [
        pgrx::datum::DatumWithOid::from(TABLE_JOB_LOCK_NAMESPACE),
        pgrx::datum::DatumWithOid::from(table_key),
    ];
    pgrx::Spi::run_with_args(
        "SELECT pg_advisory_xact_lock($1::integer, $2::integer)",
        &args,
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
    let table_key = table_oid.to_u32() as i32;
    let acquired = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT pg_try_advisory_xact_lock($1::integer, $2::integer)",
        &[
            pgrx::datum::DatumWithOid::from(TABLE_JOB_LOCK_NAMESPACE),
            pgrx::datum::DatumWithOid::from(table_key),
        ],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    Ok(acquired)
}
