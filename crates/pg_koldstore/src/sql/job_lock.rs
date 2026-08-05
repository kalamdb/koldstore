//! PostgreSQL advisory locks for table-scoped job execution.
//!
//! The durable jobs catalog prevents duplicate active rows. These **session-level**
//! locks are the primary ownership signal for flush executors: they survive
//! short commits between batches and are released on explicit unlock, backend
//! exit, or crash.
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

/// Session-level table job ownership guard.
///
/// Unlocks on drop so manage/flush/drop paths cannot leak the lock across
/// statement boundaries when using session advisory locks.
pub struct TableJobLockGuard {
    table_oid: pgrx::pg_sys::Oid,
    held: bool,
}

impl TableJobLockGuard {
    /// Blocks until the session lock is acquired.
    ///
    /// # Errors
    ///
    /// Returns an error when PostgreSQL cannot evaluate the advisory lock query.
    pub fn lock(table_oid: pgrx::pg_sys::Oid) -> Result<Self, String> {
        lock_table_job(table_oid)?;
        Ok(Self {
            table_oid,
            held: true,
        })
    }

    /// Attempts a non-blocking acquire.
    ///
    /// Returns `Ok(None)` when another backend holds the lock.
    ///
    /// # Errors
    ///
    /// Returns an error when PostgreSQL cannot evaluate the advisory lock query.
    pub fn try_lock(table_oid: pgrx::pg_sys::Oid) -> Result<Option<Self>, String> {
        if try_lock_table_job(table_oid)? {
            Ok(Some(Self {
                table_oid,
                held: true,
            }))
        } else {
            Ok(None)
        }
    }

    /// Table OID this guard owns.
    #[must_use]
    pub fn table_oid(&self) -> pgrx::pg_sys::Oid {
        self.table_oid
    }

    /// Releases ownership without waiting for `Drop`.
    pub fn unlock(mut self) {
        self.release();
    }

    fn release(&mut self) {
        if !self.held {
            return;
        }
        self.held = false;
        if let Err(error) = unlock_table_job(self.table_oid) {
            pgrx::warning!(
                "koldstore: failed to release table job lock oid={}: {error}",
                self.table_oid.to_u32()
            );
        }
    }
}

impl Drop for TableJobLockGuard {
    fn drop(&mut self) {
        self.release();
    }
}

/// Takes a session-scoped lock for flush/migration work on one table.
///
/// Blocks until the lock is available. Used by `manage_table` / DROP cleanup
/// and by flush after a successful try-lock (re-entrant). Manual `flush_table`
/// fail-fasts via [`try_lock_table_job`] instead of waiting here.
///
/// Prefer [`TableJobLockGuard`] so unlock cannot be skipped on error paths.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot evaluate the advisory lock query.
pub fn lock_table_job(table_oid: pgrx::pg_sys::Oid) -> Result<(), String> {
    let key = table_job_advisory_lock_key(table_oid.to_u32());
    pgrx::Spi::run_with_args(
        "SELECT pg_advisory_lock($1::bigint)",
        &[pgrx::datum::DatumWithOid::from(key)],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Attempts a non-blocking session table job lock.
///
/// Returns `true` when this backend now holds the lock (including when the
/// same backend already held it — PostgreSQL increments the lock count).
/// Returns `false` when another backend owns the table.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot evaluate the advisory lock query.
pub fn try_lock_table_job(table_oid: pgrx::pg_sys::Oid) -> Result<bool, String> {
    let key = table_job_advisory_lock_key(table_oid.to_u32());
    let acquired = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT pg_try_advisory_lock($1::bigint)",
        &[pgrx::datum::DatumWithOid::from(key)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    Ok(acquired)
}

/// Releases one level of session table job lock ownership.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot evaluate the unlock query.
pub fn unlock_table_job(table_oid: pgrx::pg_sys::Oid) -> Result<(), String> {
    let key = table_job_advisory_lock_key(table_oid.to_u32());
    let released = pgrx::Spi::get_one_with_args::<bool>(
        "SELECT pg_advisory_unlock($1::bigint)",
        &[pgrx::datum::DatumWithOid::from(key)],
    )
    .map_err(|error| error.to_string())?
    .unwrap_or(false);
    if !released {
        return Err(format!(
            "table job lock was not held for oid={}",
            table_oid.to_u32()
        ));
    }
    Ok(())
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
