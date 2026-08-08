//! Shared helpers for probing and waking background workers by PID.

use pgrx::pg_sys;

/// Returns whether `pid` still names a live background worker.
#[must_use]
pub(crate) fn background_worker_alive(pid: i32, database_oid: Option<u32>) -> bool {
    background_worker_proc(pid, database_oid).is_some()
}

/// Sets `procLatch` when `pid` still names a live background worker.
#[must_use]
pub(crate) fn set_background_worker_latch(pid: i32, database_oid: Option<u32>) -> bool {
    let Some(process) = background_worker_proc(pid, database_oid) else {
        return false;
    };
    unsafe {
        pg_sys::SetLatch(&raw mut (*process).procLatch);
    }
    true
}

fn background_worker_proc(pid: i32, database_oid: Option<u32>) -> Option<*mut pg_sys::PGPROC> {
    unsafe {
        let process = pg_sys::BackendPidGetProc(pid);
        if process.is_null() || (*process).pid != pid || !is_background_worker(process) {
            return None;
        }
        if let Some(database_oid) = database_oid {
            if (*process).databaseId.to_u32() != database_oid {
                return None;
            }
        }
        Some(process)
    }
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
unsafe fn is_background_worker(process: *mut pg_sys::PGPROC) -> bool {
    unsafe { (*process).isBackgroundWorker }
}

#[cfg(feature = "pg18")]
unsafe fn is_background_worker(process: *mut pg_sys::PGPROC) -> bool {
    unsafe { !(*process).isRegularBackend }
}
