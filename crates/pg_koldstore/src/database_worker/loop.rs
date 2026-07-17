//! Latch poll loop and signal handling for the async mirror applier.

use std::ffi::CString;
use std::time::Duration;

use koldstore_worker::{DatabaseWorkerTask, TickResult, APPLY_POLL_INTERVAL_MS};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};

use crate::async_mirror::task::AsyncMirrorTask;

/// Runs the persistent async-mirror apply loop until the slot disappears or shutdown.
pub(crate) fn run_async_mirror_applier(database_oid: u32) {
    attach_applier_signal_handlers();
    BackgroundWorker::connect_worker_to_spi_by_oid(
        Some(pgrx::pg_sys::Oid::from(database_oid)),
        None,
    );

    let task = AsyncMirrorTask::new(database_oid);
    let poll = Duration::from_millis(APPLY_POLL_INTERVAL_MS);
    let slot = crate::async_mirror::lifecycle::slot_name(database_oid);
    let slot_c = CString::new(slot.as_str()).expect("deterministic slot name contains no NUL");
    // Slot must already exist (manage/launcher only start us when it does). Do not
    // wait forever: after disable_async_mirror the launcher must be able to leave
    // the applier stopped.
    if !crate::async_mirror::lifecycle::native_slot_exists_cstr(&slot_c) {
        return;
    }

    let mut last_checked_wal = None;
    loop {
        let keep_running = crate::async_mirror::lifecycle::native_slot_exists_cstr(&slot_c);
        if keep_running {
            let current_wal = current_wal_position();
            if last_checked_wal != Some(current_wal) {
                worker_transaction(|| match task.tick() {
                    Ok(TickResult::Continue) => {}
                    Ok(TickResult::Stop) => {}
                    Err(error) => {
                        pgrx::error!("async mirror background apply failed: {error}")
                    }
                });
                // Capture WAL after commit so SPI-generated WAL does not wake the loop again.
                last_checked_wal = Some(current_wal_position());
            }
        }
        if !keep_running || !BackgroundWorker::wait_latch(Some(poll)) {
            break;
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
    }
}

fn attach_applier_signal_handlers() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP);
    // Use PostgreSQL's standard SIGTERM handler while logical decoding is in
    // C code. It marks interrupts pending, allowing decoding and SPI safe
    // points to abort the transaction promptly during shutdown.
    unsafe {
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        pgrx::pg_sys::pqsignal(pgrx::pg_sys::SIGTERM as i32, Some(applier_sigterm));
        #[cfg(feature = "pg18")]
        pgrx::pg_sys::pqsignal_be(pgrx::pg_sys::SIGTERM as i32, Some(applier_sigterm));
    }
}

unsafe extern "C-unwind" fn applier_sigterm(signal: std::os::raw::c_int) {
    unsafe { pgrx::pg_sys::die(signal) }
}

/// Runs `body` in a worker transaction that does not use pgrx's guarded helper.
///
/// pgrx's guarded worker transaction assigns an XID even to a read-only poll,
/// which would make the applier continuously wake itself via its own commits.
pub(crate) fn worker_transaction<R>(body: impl FnOnce() -> R) -> R {
    unsafe {
        pgrx::pg_sys::SetCurrentStatementStartTimestamp();
        pgrx::pg_sys::StartTransactionCommand();
        pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot());
    }
    let result = body();
    unsafe {
        pgrx::pg_sys::PopActiveSnapshot();
        pgrx::pg_sys::CommitTransactionCommand();
    }
    result
}

fn current_wal_position() -> u64 {
    unsafe { pgrx::pg_sys::GetXLogInsertRecPtr() }
}
