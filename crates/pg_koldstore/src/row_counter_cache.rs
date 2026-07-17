//! Backend-local row counter deltas flushed to `koldstore.manifest` on commit.
//!
//! PERFORMANCE: DML capture triggers call `internal_record_row_count_delta`, which
//! only updates this in-memory map. One manifest UPDATE per touched table runs on
//! transaction commit (not once per row), so heavy write workloads avoid hot-path IO.

#[cfg(feature = "pg")]
use std::cell::RefCell;
#[cfg(feature = "pg")]
use std::collections::HashMap;

#[cfg(feature = "pg")]
use pgrx::pg_sys;

#[cfg(feature = "pg")]
thread_local! {
    static PENDING_ROW_COUNT_DELTAS: RefCell<HashMap<u32, (i64, i64)>> =
        RefCell::new(HashMap::new());
}

/// Records hot/mirror counter deltas for one managed table in backend memory.
#[cfg(feature = "pg")]
pub fn record_delta(table_oid: pg_sys::Oid, hot_delta: i64, mirror_delta: i64) {
    if hot_delta == 0 && mirror_delta == 0 {
        return;
    }
    let table_oid = table_oid.to_u32();
    PENDING_ROW_COUNT_DELTAS.with(|pending| {
        let mut pending = pending.borrow_mut();
        let entry = pending.entry(table_oid).or_insert((0, 0));
        entry.0 = entry.0.saturating_add(hot_delta);
        entry.1 = entry.1.saturating_add(mirror_delta);
    });
}

/// Returns uncommitted `(hot_delta, mirror_delta)` for one table in this backend.
///
/// Flush may call `apply_available` and then read O(1) manifest counters in the
/// same transaction. Those counters stay stale until pre-commit unless callers
/// fold these pending deltas into the read.
#[cfg(feature = "pg")]
#[must_use]
pub fn pending_deltas(table_oid: pg_sys::Oid) -> (i64, i64) {
    let table_oid = table_oid.to_u32();
    PENDING_ROW_COUNT_DELTAS
        .with(|pending| pending.borrow().get(&table_oid).copied().unwrap_or((0, 0)))
}

/// Takes pending deltas, restoring them on failure so a retry can still flush.
#[cfg(feature = "pg")]
fn take_pending_deltas() -> Vec<(u32, (i64, i64))> {
    PENDING_ROW_COUNT_DELTAS.with(|pending| pending.borrow_mut().drain().collect())
}

/// Restores deltas after a failed flush attempt (merge with any newly recorded values).
#[cfg(feature = "pg")]
fn restore_pending_deltas(deltas: Vec<(u32, (i64, i64))>) {
    PENDING_ROW_COUNT_DELTAS.with(|pending| {
        let mut pending = pending.borrow_mut();
        for (table_oid, (hot_delta, mirror_delta)) in deltas {
            let entry = pending.entry(table_oid).or_insert((0, 0));
            entry.0 = entry.0.saturating_add(hot_delta);
            entry.1 = entry.1.saturating_add(mirror_delta);
        }
    });
}

/// Flushes pending counter deltas with ordinary SPI while a transaction is open.
///
/// Async WAL apply must call this before returning: the background worker uses a
/// custom `StartTransactionCommand` / `CommitTransactionCommand` path where
/// relying solely on `XACT_EVENT_PRE_COMMIT` can leave counters at zero after the
/// worker wins the apply race against `wait_for_async_mirror`.
///
/// # Errors
///
/// Returns an error when SQL cannot be prepared or a bump statement fails. On
/// failure, unapplied pending deltas are restored so a later retry can flush them.
#[cfg(feature = "pg")]
pub fn flush_pending_deltas_in_transaction() -> Result<(), String> {
    use pgrx::datum::DatumWithOid;

    let mut deltas = take_pending_deltas();
    if deltas.is_empty() {
        return Ok(());
    }

    let statement = match koldstore_flush::plan_bump_table_row_counts() {
        Ok(statement) => statement,
        Err(error) => {
            restore_pending_deltas(deltas);
            return Err(format!("prepare row counter bump SQL: {error}"));
        }
    };

    while let Some((table_oid, (hot_delta, mirror_delta))) = deltas.first().copied() {
        if hot_delta != 0 || mirror_delta != 0 {
            if let Err(error) = crate::spi::update(
                &statement,
                &[
                    DatumWithOid::from(pg_sys::Oid::from(table_oid)),
                    DatumWithOid::from(hot_delta),
                    DatumWithOid::from(mirror_delta),
                ],
            ) {
                restore_pending_deltas(deltas);
                return Err(format!(
                    "bump row counters for table oid {table_oid}: {error}"
                ));
            }
        }
        deltas.remove(0);
    }
    Ok(())
}

/// Applies pending counter deltas to `koldstore.manifest` at transaction pre-commit.
///
/// SPI must run while the transaction is still open; `XACT_EVENT_COMMIT` is too late
/// and panics when `Spi::connect` runs from the xact callback. Unapplied deltas are
/// restored when a bump fails so they are not silently dropped.
#[cfg(feature = "pg")]
pub fn flush_pending_deltas() {
    use pgrx::datum::DatumWithOid;

    let mut deltas = take_pending_deltas();
    if deltas.is_empty() {
        return;
    }

    let statement = match koldstore_flush::plan_bump_table_row_counts() {
        Ok(statement) => statement,
        Err(error) => {
            restore_pending_deltas(deltas);
            pgrx::warning!("koldstore row counter flush failed to prepare SQL: {error}");
            return;
        }
    };

    while let Some((table_oid, (hot_delta, mirror_delta))) = deltas.first().copied() {
        if hot_delta != 0 || mirror_delta != 0 {
            if let Err(error) = crate::spi::update_in_xact_callback(
                &statement,
                &[
                    DatumWithOid::from(pg_sys::Oid::from(table_oid)),
                    DatumWithOid::from(hot_delta),
                    DatumWithOid::from(mirror_delta),
                ],
            ) {
                restore_pending_deltas(deltas);
                pgrx::warning!(
                    "koldstore row counter flush failed for table oid {table_oid}: {error}"
                );
                return;
            }
        }
        deltas.remove(0);
    }
}

/// Discards pending counter deltas after transaction abort.
#[cfg(feature = "pg")]
pub fn clear_pending_deltas() {
    PENDING_ROW_COUNT_DELTAS.with(|pending| pending.borrow_mut().clear());
}

/// Registers a permanent transaction callback that flushes or clears pending deltas.
#[cfg(feature = "pg")]
pub fn register_xact_callbacks() {
    unsafe {
        pg_sys::RegisterXactCallback(Some(row_counter_xact_callback), std::ptr::null_mut());
    }
}

#[cfg(feature = "pg")]
#[pgrx::pg_guard]
unsafe extern "C-unwind" fn row_counter_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_PRE_COMMIT => {
            flush_pending_deltas();
        }
        pg_sys::XactEvent::XACT_EVENT_ABORT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT => {
            clear_pending_deltas();
        }
        _ => {}
    }
}

#[cfg(not(feature = "pg"))]
pub fn record_delta(_table_oid: u32, _hot_delta: i64, _mirror_delta: i64) {}

#[cfg(not(feature = "pg"))]
#[must_use]
pub fn pending_deltas(_table_oid: u32) -> (i64, i64) {
    (0, 0)
}

#[cfg(not(feature = "pg"))]
pub fn flush_pending_deltas() {}

#[cfg(not(feature = "pg"))]
pub fn flush_pending_deltas_in_transaction() -> Result<(), String> {
    Ok(())
}

#[cfg(not(feature = "pg"))]
pub fn clear_pending_deltas() {}

#[cfg(not(feature = "pg"))]
pub fn register_xact_callbacks() {}
