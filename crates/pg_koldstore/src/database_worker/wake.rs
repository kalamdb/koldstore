//! Commit-driven, coalescing wakeups for database-scoped WAL appliers.
//!
//! Foreground backends set one transaction-local dirty bit when a managed
//! relation is changed. After commit, a fixed shared-memory registry advances
//! that database's generation and sets the worker latch. Generations preserve
//! work across worker startup/restart races while the latch naturally
//! coalesces concurrent commits into one wake.

use std::cell::RefCell;

use koldstore_worker::{AtomicWakeRegistry, TransactionDirty, WakeGeneration, WorkerPid};
use pgrx::{pg_guard, pg_shmem_init, pg_sys, AssertPGRXSharedMemory, PgAtomic};

const WAKE_REGISTRY_CAPACITY: usize = 256;
type SharedWakeRegistry = AssertPGRXSharedMemory<AtomicWakeRegistry<WAKE_REGISTRY_CAPACITY>>;

static WAKE_REGISTRY: PgAtomic<SharedWakeRegistry> =
    unsafe { PgAtomic::new(c"koldstore async wake registry") };

thread_local! {
    static MANAGED_DML_PENDING: RefCell<TransactionDirty> =
        RefCell::new(TransactionDirty::default());
}

/// Allocates the shared registry and registers the post-commit publisher.
#[allow(unexpected_cfgs)] // pgrx's macro includes supported PG features this crate omits.
pub(crate) fn initialize() {
    pg_shmem_init!(
        WAKE_REGISTRY = unsafe { AssertPGRXSharedMemory::new(AtomicWakeRegistry::default()) }
    );
    unsafe {
        pg_sys::RegisterXactCallback(Some(wake_xact_callback), std::ptr::null_mut());
        pg_sys::RegisterSubXactCallback(Some(wake_subxact_callback), std::ptr::null_mut());
    }
}

/// Marks the current transaction as containing managed-table source DML.
pub(crate) fn mark_managed_dml_pending() {
    if crate::sql::flush::spi::flush_replication_origin_is_armed() {
        return;
    }
    let nesting_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() }.max(1) as u32;
    MANAGED_DML_PENDING.with(|pending| pending.borrow_mut().mark(nesting_level));
}

/// Registers the current background worker and returns the current generation.
pub(crate) fn register_worker(database_oid: u32) -> Option<WakeGeneration> {
    let pid = WorkerPid::new(unsafe { pgrx::pg_sys::MyProcPid });
    WAKE_REGISTRY.get().register_worker(database_oid, pid)
}

/// Clears the current worker PID while retaining its generation.
pub(crate) fn unregister_worker(database_oid: u32) {
    let pid = WorkerPid::new(unsafe { pgrx::pg_sys::MyProcPid });
    WAKE_REGISTRY.get().unregister_worker(database_oid, pid);
}

/// Reads the latest committed generation for a database.
#[must_use]
pub(crate) fn generation(database_oid: u32) -> WakeGeneration {
    WAKE_REGISTRY
        .get()
        .generation(database_oid)
        .unwrap_or_else(|| WakeGeneration::new(0))
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn wake_xact_callback(
    event: pgrx::pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    match event {
        pgrx::pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => publish_pending_commit(),
        pgrx::pg_sys::XactEvent::XACT_EVENT_ABORT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PREPARE
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PRE_PREPARE => clear_pending(),
        _ => {}
    }
}

fn clear_pending() {
    MANAGED_DML_PENDING.with(|pending| pending.borrow_mut().clear());
}

fn publish_pending_commit() {
    let pending = MANAGED_DML_PENDING.with(|pending| pending.borrow_mut().take());
    if !pending {
        return;
    }

    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    let published = WAKE_REGISTRY.get().publish(database_oid);
    let Some(worker_pid) = published.and_then(|wake| wake.worker_pid) else {
        return;
    };
    wake_worker(database_oid, worker_pid);
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn wake_subxact_callback(
    event: pgrx::pg_sys::SubXactEvent::Type,
    _my_subid: pgrx::pg_sys::SubTransactionId,
    _parent_subid: pgrx::pg_sys::SubTransactionId,
    _arg: *mut std::ffi::c_void,
) {
    let nesting_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() }.max(1) as u32;
    MANAGED_DML_PENDING.with(|pending| match event {
        pg_sys::SubXactEvent::SUBXACT_EVENT_COMMIT_SUB => {
            pending.borrow_mut().commit_subtransaction(nesting_level)
        }
        pg_sys::SubXactEvent::SUBXACT_EVENT_ABORT_SUB => {
            pending.borrow_mut().abort_subtransaction(nesting_level)
        }
        _ => {}
    });
}

fn wake_worker(database_oid: u32, worker_pid: WorkerPid) {
    unsafe {
        let process = pgrx::pg_sys::BackendPidGetProc(worker_pid.get());
        if process.is_null()
            || (*process).pid != worker_pid.get()
            || (*process).databaseId.to_u32() != database_oid
            || !(*process).isBackgroundWorker
        {
            return;
        }
        pgrx::pg_sys::SetLatch(&raw mut (*process).procLatch);
    }
}
