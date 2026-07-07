//! PostgreSQL advisory locks for table-scoped job execution.
//!
//! The durable jobs catalog prevents duplicate active rows. These transaction
//! locks add an early, low-cost guard around inline SQL entrypoints before they
//! perform setup work.

const TABLE_JOB_LOCK_NAMESPACE: i32 = 0x4b54_4a42;

/// Takes a transaction-scoped lock for flush/migration work on one table.
///
/// # Errors
///
/// Returns an error when another transaction already owns table-scoped
/// KoldStore work for the same relation, or when PostgreSQL cannot evaluate the
/// advisory lock query.
pub fn lock_table_job(table_oid: pgrx::pg_sys::Oid) -> Result<(), String> {
    let table_key = table_oid.to_u32() as i32;
    let args = [
        pgrx::datum::DatumWithOid::from(TABLE_JOB_LOCK_NAMESPACE),
        pgrx::datum::DatumWithOid::from(table_key),
    ];
    let acquired = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT pg_try_advisory_xact_lock($1::integer, $2::integer)",
        &args,
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if acquired {
        Ok(())
    } else {
        Err("another KoldStore flush or migration is already active for this table".to_string())
    }
}
