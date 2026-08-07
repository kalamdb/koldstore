//! Commit-driven, coalescing wakeups for KoldStore background work.
//!
//! Wakeups are latency hints only. The durable sources of truth remain the
//! logical replication slot / async_mirror_state and koldstore.jobs. Foreground
//! transactions publish monotonically increasing shared generations only after
//! top-level commit, then wake the single postmaster-supervised cluster process.

use std::cell::RefCell;

use koldstore_worker::{
    DatabaseWorkSnapshot, EnsurePauseSet, SupervisorPid, SupervisorRegistry, TransactionDirty,
    SUPERVISOR_REGISTRY_CAPACITY,
};
use pgrx::{pg_guard, pg_shmem_init, pg_sys, AssertPGRXSharedMemory, PgAtomic};

type SharedEnsurePauseSet = AssertPGRXSharedMemory<EnsurePauseSet<SUPERVISOR_REGISTRY_CAPACITY>>;
type SharedSupervisorRegistry =
    AssertPGRXSharedMemory<SupervisorRegistry<SUPERVISOR_REGISTRY_CAPACITY>>;

// Test/benchmark pause compatibility. Production scheduling does not depend on it.
static ENSURE_PAUSE_SET: PgAtomic<SharedEnsurePauseSet> =
    unsafe { PgAtomic::new(c"koldstore async ensure pause set") };
static SUPERVISOR_REGISTRY: PgAtomic<SharedSupervisorRegistry> =
    unsafe { PgAtomic::new(c"koldstore supervisor registry") };

thread_local! {
    static MANAGED_DML_PENDING: RefCell<TransactionDirty> =
        RefCell::new(TransactionDirty::default());
    static FLUSH_QUEUE_PENDING: RefCell<TransactionDirty> =
        RefCell::new(TransactionDirty::default());
    static SCHEDULE_PENDING: RefCell<TransactionDirty> =
        RefCell::new(TransactionDirty::default());
}

#[allow(unexpected_cfgs)]
pub(crate) fn initialize() {
    pg_shmem_init!(
        ENSURE_PAUSE_SET = unsafe { AssertPGRXSharedMemory::new(EnsurePauseSet::default()) }
    );
    pg_shmem_init!(
        SUPERVISOR_REGISTRY = unsafe { AssertPGRXSharedMemory::new(SupervisorRegistry::default()) }
    );
    unsafe {
        pg_sys::RegisterXactCallback(Some(wake_xact_callback), std::ptr::null_mut());
        pg_sys::RegisterSubXactCallback(Some(wake_subxact_callback), std::ptr::null_mut());
    }
}

pub(crate) fn pause_ensure(database_oid: u32) -> bool {
    ENSURE_PAUSE_SET.get().pause(database_oid)
}

pub(crate) fn resume_ensure(database_oid: u32) {
    ENSURE_PAUSE_SET.get().resume(database_oid);
}

#[must_use]
pub(crate) fn ensure_paused(database_oid: u32) -> bool {
    ENSURE_PAUSE_SET.get().is_paused(database_oid)
}

/// Marks one transaction as containing source WAL that may affect KoldStore.
/// Multiple statements/rows collapse to one generation on successful commit.
pub(crate) fn mark_managed_dml_pending() {
    if is_current_backend_background_worker() {
        return;
    }
    if crate::sql::flush::spi::flush_replication_origin_is_armed() {
        return;
    }
    MANAGED_DML_PENDING.with(|pending| pending.borrow_mut().mark(current_nesting_level()));
}

/// Marks a durable flush queue mutation. Background maintenance workers may use
/// this because auto-flush enqueue must wake the supervisor after its own commit.
pub(crate) fn mark_flush_queue_pending() {
    FLUSH_QUEUE_PENDING.with(|pending| pending.borrow_mut().mark(current_nesting_level()));
}

/// Marks database scheduling/recovery metadata dirty for post-commit dispatch.
pub(crate) fn mark_schedule_pending() {
    SCHEDULE_PENDING.with(|pending| pending.borrow_mut().mark(current_nesting_level()));
}

pub(crate) fn register_supervisor() {
    SUPERVISOR_REGISTRY
        .get()
        .register_supervisor(SupervisorPid::new(unsafe { pg_sys::MyProcPid }));
}

pub(crate) fn unregister_supervisor() {
    SUPERVISOR_REGISTRY
        .get()
        .unregister_supervisor(SupervisorPid::new(unsafe { pg_sys::MyProcPid }));
}

pub(crate) fn wake_supervisor() {
    if let Some(pid) = SUPERVISOR_REGISTRY.get().supervisor_pid() {
        set_background_worker_latch(pid.get(), None);
    }
}

/// Direct recovery request used by crash/startup paths outside a client commit.
pub(crate) fn request_recovery(database_oid: u32) {
    if let Some(pid) = SUPERVISOR_REGISTRY.get().request_recovery(database_oid) {
        set_background_worker_latch(pid.get(), None);
    }
}

/// Converts a reached queue deadline into a new flush generation immediately.
/// The caller is the supervisor, so this does not need a transaction callback.
pub(crate) fn publish_due_flush(database_oid: u32) {
    let _ = SUPERVISOR_REGISTRY.get().publish_flush(database_oid);
}

#[must_use]
pub(crate) fn supervisor_snapshots() -> Vec<DatabaseWorkSnapshot> {
    SUPERVISOR_REGISTRY.get().snapshots()
}

#[must_use]
pub(crate) fn supervisor_snapshot(database_oid: u32) -> Option<DatabaseWorkSnapshot> {
    SUPERVISOR_REGISTRY.get().snapshot(database_oid)
}

pub(crate) fn try_reserve_maintenance(database_oid: u32) -> bool {
    SUPERVISOR_REGISTRY
        .get()
        .try_reserve_maintenance(database_oid)
}

pub(crate) fn maintenance_started(database_oid: u32) -> bool {
    SUPERVISOR_REGISTRY
        .get()
        .maintenance_started(database_oid, unsafe { pg_sys::MyProcPid })
}

pub(crate) fn cancel_maintenance_start(database_oid: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .cancel_maintenance_start(database_oid);
}

pub(crate) fn clear_stale_maintenance(database_oid: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .clear_stale_maintenance(database_oid);
}

pub(crate) fn maintenance_stopped(database_oid: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .maintenance_stopped(database_oid, unsafe { pg_sys::MyProcPid });
    wake_supervisor();
}

pub(crate) fn mark_wal_processed(database_oid: u32, generation: u64) {
    SUPERVISOR_REGISTRY
        .get()
        .mark_wal_processed(database_oid, generation);
}

pub(crate) fn mark_maintenance_reconciled(database_oid: u32, generation: u64) {
    SUPERVISOR_REGISTRY
        .get()
        .mark_maintenance_reconciled(database_oid, generation);
}

pub(crate) fn set_flush_limit(database_oid: u32, limit: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .set_flush_limit(database_oid, limit);
}

pub(crate) fn try_reserve_flush(database_oid: u32, cluster_limit: u32) -> bool {
    SUPERVISOR_REGISTRY
        .get()
        .try_reserve_flush(database_oid, cluster_limit)
}

pub(crate) fn flush_started(database_oid: u32, effective_limit: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .flush_started(database_oid, effective_limit);
    // The first worker teaches the supervisor this DB's effective cap. Wake it
    // immediately so a queue can fan out without waiting for that worker to exit.
    wake_supervisor();
}

pub(crate) fn cancel_flush_start(database_oid: u32) {
    SUPERVISOR_REGISTRY.get().cancel_flush_start(database_oid);
}

pub(crate) fn reconcile_flush_counts(database_oid: u32, running: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .reconcile_flush_counts(database_oid, running);
}

pub(crate) fn flush_stopped(database_oid: u32) {
    SUPERVISOR_REGISTRY.get().flush_stopped(database_oid);
    wake_supervisor();
}

pub(crate) fn mark_flush_processed(database_oid: u32, generation: u64) {
    SUPERVISOR_REGISTRY
        .get()
        .mark_flush_processed(database_oid, generation);
}

pub(crate) fn schedule_flush_at_ms(database_oid: u32, deadline_ms: i64) {
    SUPERVISOR_REGISTRY
        .get()
        .schedule_flush_at_ms(database_oid, deadline_ms);
    wake_supervisor();
}

pub(crate) fn clear_flush_deadline(database_oid: u32) {
    SUPERVISOR_REGISTRY.get().clear_flush_deadline(database_oid);
}

pub(crate) fn consume_flush_deadline(database_oid: u32, sampled_ms: i64) -> bool {
    SUPERVISOR_REGISTRY
        .get()
        .consume_flush_deadline(database_oid, sampled_ms)
}

#[must_use]
pub(crate) fn overflow_reconcile_required() -> bool {
    SUPERVISOR_REGISTRY.get().overflow_reconcile_required()
}

pub(crate) fn clear_overflow_reconcile_required() {
    SUPERVISOR_REGISTRY
        .get()
        .clear_overflow_reconcile_required();
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn wake_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::ffi::c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_COMMIT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => {
            publish_pending_commit()
        }
        pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PREPARE
        | pg_sys::XactEvent::XACT_EVENT_PRE_PREPARE => clear_pending(),
        _ => {}
    }
}

fn clear_pending() {
    MANAGED_DML_PENDING.with(|pending| pending.borrow_mut().clear());
    FLUSH_QUEUE_PENDING.with(|pending| pending.borrow_mut().clear());
    SCHEDULE_PENDING.with(|pending| pending.borrow_mut().clear());
}

fn publish_pending_commit() {
    let wal_pending = MANAGED_DML_PENDING.with(|pending| pending.borrow_mut().take());
    let flush_pending = FLUSH_QUEUE_PENDING.with(|pending| pending.borrow_mut().take());
    let schedule_pending = SCHEDULE_PENDING.with(|pending| pending.borrow_mut().take());
    if !wal_pending && !flush_pending && !schedule_pending {
        return;
    }

    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let mut supervisor = None;

    if wal_pending && !is_current_backend_background_worker() {
        // Filtering happens after commit and uses PostgreSQL's native replication
        // slot shared-memory lookup, never SPI/catalog SQL. This lets ExecutorEnd
        // conservatively mark nested/trigger/cascade DML without waking KoldStore
        // in databases that have no async capture slot.
        let slot = crate::mirror::lifecycle::slot_name(database_oid);
        if crate::mirror::lifecycle::native_slot_exists(&slot) {
            supervisor = SUPERVISOR_REGISTRY.get().publish_wal(database_oid);
            // If a burst worker is already alive, wake it too. The generation is
            // authoritative, so a stale PID or missed SetLatch cannot lose work.
            if let Some(snapshot) = SUPERVISOR_REGISTRY.get().snapshot(database_oid) {
                if snapshot.maintenance_pid > 0 {
                    set_background_worker_latch(snapshot.maintenance_pid, Some(database_oid));
                }
            }
        }
    }
    if flush_pending {
        supervisor = SUPERVISOR_REGISTRY
            .get()
            .publish_flush(database_oid)
            .or(supervisor);
    }
    if schedule_pending {
        supervisor = SUPERVISOR_REGISTRY
            .get()
            .publish_schedule(database_oid)
            .or(supervisor);
    }

    if let Some(pid) = supervisor {
        set_background_worker_latch(pid.get(), None);
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn wake_subxact_callback(
    event: pg_sys::SubXactEvent::Type,
    _my_subid: pg_sys::SubTransactionId,
    _parent_subid: pg_sys::SubTransactionId,
    _arg: *mut std::ffi::c_void,
) {
    let nesting_level = current_nesting_level();
    update_subxact_dirty(&MANAGED_DML_PENDING, event, nesting_level);
    update_subxact_dirty(&FLUSH_QUEUE_PENDING, event, nesting_level);
    update_subxact_dirty(&SCHEDULE_PENDING, event, nesting_level);
}

fn update_subxact_dirty(
    dirty: &'static std::thread::LocalKey<RefCell<TransactionDirty>>,
    event: pg_sys::SubXactEvent::Type,
    nesting_level: u32,
) {
    dirty.with(|pending| match event {
        pg_sys::SubXactEvent::SUBXACT_EVENT_COMMIT_SUB => {
            pending.borrow_mut().commit_subtransaction(nesting_level)
        }
        pg_sys::SubXactEvent::SUBXACT_EVENT_ABORT_SUB => {
            pending.borrow_mut().abort_subtransaction(nesting_level)
        }
        _ => {}
    });
}

fn current_nesting_level() -> u32 {
    unsafe { pg_sys::GetCurrentTransactionNestLevel() }.max(1) as u32
}

fn set_background_worker_latch(pid: i32, database_oid: Option<u32>) {
    unsafe {
        let process = pg_sys::BackendPidGetProc(pid);
        if process.is_null() || (*process).pid != pid || !is_background_worker(process) {
            return;
        }
        if let Some(database_oid) = database_oid {
            if (*process).databaseId.to_u32() != database_oid {
                return;
            }
        }
        pg_sys::SetLatch(&raw mut (*process).procLatch);
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

fn is_current_backend_background_worker() -> bool {
    unsafe { !pg_sys::MyBgworkerEntry.is_null() }
}
