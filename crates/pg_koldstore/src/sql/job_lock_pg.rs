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
