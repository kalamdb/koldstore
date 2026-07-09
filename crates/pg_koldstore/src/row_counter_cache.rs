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

/// Applies pending counter deltas to `koldstore.manifest` at transaction pre-commit.
///
/// SPI must run while the transaction is still open; `XACT_EVENT_COMMIT` is too late
/// and panics when `Spi::connect` runs from the xact callback.
#[cfg(feature = "pg")]
pub fn flush_pending_deltas() {
    use pgrx::datum::DatumWithOid;

    let deltas =
        PENDING_ROW_COUNT_DELTAS.with(|pending| pending.borrow_mut().drain().collect::<Vec<_>>());
    if deltas.is_empty() {
        return;
    }

    let statement = match koldstore_flush::plan_bump_table_row_counts() {
        Ok(statement) => statement,
        Err(error) => {
            pgrx::warning!("koldstore row counter flush failed to prepare SQL: {error}");
            return;
        }
    };

    for (table_oid, (hot_delta, mirror_delta)) in deltas {
        if hot_delta == 0 && mirror_delta == 0 {
            continue;
        }
        if let Err(error) = crate::spi::update_in_xact_callback(
            &statement,
            &[
                DatumWithOid::from(pg_sys::Oid::from(table_oid)),
                DatumWithOid::from(hot_delta),
                DatumWithOid::from(mirror_delta),
            ],
        ) {
            pgrx::warning!("koldstore row counter flush failed for table oid {table_oid}: {error}");
        }
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
pub fn flush_pending_deltas() {}

#[cfg(not(feature = "pg"))]
pub fn clear_pending_deltas() {}

#[cfg(not(feature = "pg"))]
pub fn register_xact_callbacks() {}
