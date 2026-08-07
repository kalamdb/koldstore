//! Commit-driven, coalescing wakeups for KoldStore background work.
//!
//! Wakeups are latency hints only.  The durable sources of truth remain the
//! logical slot / async_mirror_state and koldstore.jobs.  Transaction-local
//! dirty bits publish monotonically increasing shared-memory generations only
//! after top-level commit, then set the single cluster supervisor latch.
//!
//! The older per-database applier wake registry remains temporarily during the
//! migration so existing WAL tests keep working while dispatch moves to the
//! cluster supervisor. New queue dispatch must use [`mark_flush_queue_pending`]
//! and the supervisor registry rather than registering workers from a client.

use std::cell::RefCell;

use koldstore_worker::{
    AtomicWakeRegistry, DatabaseWorkSnapshot, EnsurePauseSet, SupervisorPid, SupervisorRegistry,
    TransactionDirty, WakeGeneration, WorkerPid, SUPERVISOR_REGISTRY_CAPACITY,
    WAKE_REGISTRY_CAPACITY,
};
use pgrx::{pg_shmem_init, pg_sys, AssertPGRXSharedMemory, PgAtomic};

type SharedWakeRegistry = AssertPGRXSharedMemory<AtomicWakeRegistry<WAKE_REGISTRY_CAPACITY>>;
type SharedEnsurePauseSet = AssertPGRXSharedMemory<EnsurePauseSet<WAKE_REGISTRY_CAPACITY>>;
type SharedSupervisorRegistry =
    AssertPGRXSharedMemory<SupervisorRegistry<SUPERVISOR_REGISTRY_CAPACITY>>;

static WAKE_REGISTRY: PgAtomic<SharedWakeRegistry> =
    unsafe { PgAtomic::new(c"koldstore async wake registry") };
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

/// Allocates shared registries and registers transaction callbacks.
#[allow(unexpected_cfgs)] // pgrx's macro includes supported PG features this crate omits.
pub(crate) fn initialize() {
    pg_shmem_init!(
        WAKE_REGISTRY = unsafe { AssertPGRXSharedMemory::new(AtomicWakeRegistry::default()) }
    );
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

/// Pauses legacy ensure/register for `database_oid` across the postmaster.
///
/// Retained only while the old per-database worker lifecycle is being removed.
pub(crate) fn pause_ensure(database_oid: u32) -> bool {
    ENSURE_PAUSE_SET.get().pause(database_oid)
}

/// Clears a legacy ensure pause for `database_oid`.
pub(crate) fn resume_ensure(database_oid: u32) {
    ENSURE_PAUSE_SET.get().resume(database_oid);
}

/// Returns whether legacy ensure must skip registration for `database_oid`.
#[must_use]
pub(crate) fn ensure_paused(database_oid: u32) -> bool {
    ENSURE_PAUSE_SET.get().is_paused(database_oid)
}

/// Marks the current transaction as containing source WAL worth applying.
///
/// One transaction, regardless of row count/statements, publishes one WAL
/// generation after commit. KoldStore's own background writes are excluded so
/// the applier cannot create a self-wake loop.
pub(crate) fn mark_managed_dml_pending() {
    if is_current_backend_background_worker() {
        return;
    }
    if crate::sql::flush::spi::flush_replication_origin_is_armed() {
        return;
    }
    let nesting_level = current_nesting_level();
    MANAGED_DML_PENDING.with(|pending| pending.borrow_mut().mark(nesting_level));
}

/// Marks a durable flush-queue mutation for post-commit supervisor dispatch.
///
/// Unlike WAL dirty state this is intentionally allowed from background workers:
/// a maintenance worker may enqueue automatic flush work and must wake the
/// supervisor only after that job row commits.
pub(crate) fn mark_flush_queue_pending() {
    let nesting_level = current_nesting_level();
    FLUSH_QUEUE_PENDING.with(|pending| pending.borrow_mut().mark(nesting_level));
}

/// Marks database scheduling metadata dirty for post-commit reconciliation.
pub(crate) fn mark_schedule_pending() {
    let nesting_level = current_nesting_level();
    SCHEDULE_PENDING.with(|pending| pending.borrow_mut().mark(nesting_level));
}

/// Registers the single static cluster supervisor in shared memory.
pub(crate) fn register_supervisor() {
    SUPERVISOR_REGISTRY
        .get()
        .register_supervisor(SupervisorPid::new(unsafe { pg_sys::MyProcPid }));
}

/// Clears the supervisor PID if it still belongs to this process.
pub(crate) fn unregister_supervisor() {
    SUPERVISOR_REGISTRY
        .get()
        .unregister_supervisor(SupervisorPid::new(unsafe { pg_sys::MyProcPid }));
}

/// Wakes the current supervisor, if registered.
pub(crate) fn wake_supervisor() {
    if let Some(pid) = SUPERVISOR_REGISTRY.get().supervisor_pid() {
        set_background_worker_latch(pid.get(), None);
    }
}

/// Requests durable crash/startup reconciliation for a database and wakes the supervisor.
pub(crate) fn request_recovery(database_oid: u32) {
    let target = SUPERVISOR_REGISTRY.get().request_recovery(database_oid);
    if let Some(pid) = target {
        set_background_worker_latch(pid.get(), None);
    }
}

/// Requests reconciliation for the current database.
pub(crate) fn request_current_database_recovery() {
    request_recovery(unsafe { pg_sys::MyDatabaseId }.to_u32());
}

/// Shared supervisor snapshots used only by the static dispatcher.
#[must_use]
pub(crate) fn supervisor_snapshots() -> Vec<DatabaseWorkSnapshot> {
    SUPERVISOR_REGISTRY.get().snapshots()
}

/// Shared snapshot for one database.
#[must_use]
pub(crate) fn supervisor_snapshot(database_oid: u32) -> Option<DatabaseWorkSnapshot> {
    SUPERVISOR_REGISTRY.get().snapshot(database_oid)
}

/// Reserves the single maintenance worker slot for a database.
pub(crate) fn try_reserve_maintenance(database_oid: u32) -> bool {
    SUPERVISOR_REGISTRY
        .get()
        .try_reserve_maintenance(database_oid)
}

/// Marks the current maintenance worker live.
pub(crate) fn maintenance_started(database_oid: u32) -> bool {
    SUPERVISOR_REGISTRY
        .get()
        .maintenance_started(database_oid, unsafe { pg_sys::MyProcPid })
}

/// Releases a maintenance registration reservation that failed before startup.
pub(crate) fn cancel_maintenance_start(database_oid: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .cancel_maintenance_start(database_oid);
}

/// Releases the current maintenance worker and wakes the supervisor.
pub(crate) fn maintenance_stopped(database_oid: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .maintenance_stopped(database_oid, unsafe { pg_sys::MyProcPid });
    wake_supervisor();
}

/// Marks a WAL generation safely processed.
pub(crate) fn mark_wal_processed(database_oid: u32, generation: u64) {
    SUPERVISOR_REGISTRY
        .get()
        .mark_wal_processed(database_oid, generation);
}

/// Clears recovery/schedule flags after a successful DB-local pass.
pub(crate) fn mark_maintenance_reconciled(database_oid: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .mark_maintenance_reconciled(database_oid);
}

/// Sets the effective per-database flush concurrency limit.
pub(crate) fn set_flush_limit(database_oid: u32, limit: u32) {
    SUPERVISOR_REGISTRY.get().set_flush_limit(database_oid, limit);
}

/// Reserves one flush worker slot, counting both Starting and Running workers.
pub(crate) fn try_reserve_flush(database_oid: u32, cluster_limit: u32) -> bool {
    SUPERVISOR_REGISTRY
        .get()
        .try_reserve_flush(database_oid, cluster_limit)
}

/// Moves one flush reservation from Starting to Running.
pub(crate) fn flush_started(database_oid: u32, effective_limit: u32) {
    SUPERVISOR_REGISTRY
        .get()
        .flush_started(database_oid, effective_limit);
}

/// Cancels a flush worker registration that never started.
pub(crate) fn cancel_flush_start(database_oid: u32) {
    SUPERVISOR_REGISTRY.get().cancel_flush_start(database_oid);
}

/// Releases one running flush worker and wakes the supervisor for immediate refill.
pub(crate) fn flush_stopped(database_oid: u32) {
    SUPERVISOR_REGISTRY.get().flush_stopped(database_oid);
    wake_supervisor();
}

/// Marks a queue generation drained without clearing a newer enqueue race.
pub(crate) fn mark_flush_processed(database_oid: u32, generation: u64) {
    SUPERVISOR_REGISTRY
        .get()
        .mark_flush_processed(database_oid, generation);
}

/// Returns whether the fixed registry overflowed and needs conservative recovery.
#[must_use]
pub(crate) fn overflow_reconcile_required() -> bool {
    SUPERVISOR_REGISTRY.get().overflow_reconcile_required()
}

/// Clears the overflow marker after an authoritative supervisor scan.
pub(crate) fn clear_overflow_reconcile_required() {
    SUPERVISOR_REGISTRY
        .get()
        .clear_overflow_reconcile_required();
}

// ---- Legacy per-database applier wake helpers (temporary migration bridge). ----

/// Registers the current legacy database worker and returns the current generation.
pub(crate) fn register_worker(database_oid: u32) -> Option<WakeGeneration> {
    let pid = WorkerPid::new(unsafe { pg_sys::MyProcPid });
    WAKE_REGISTRY.get().register_worker(database_oid, pid)
}

/// Clears the legacy worker PID while retaining its generation.
pub(crate) fn unregister_worker(database_oid: u32) {
    let pid = WorkerPid::new(unsafe { pg_sys::MyProcPid });
    WAKE_REGISTRY.get().unregister_worker(database_oid, pid);
}

/// Reads the latest legacy committed generation for a database.
#[must_use]
pub(crate) fn generation(database_oid: u32) -> WakeGeneration {
    WAKE_REGISTRY
        .get()
        .generation(database_oid)
        .unwrap_or_else(|| WakeGeneration::new(0))
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
        // Publish the new supervisor generation first. Even if no supervisor is
        // alive, the generation remains dirty for startup reconciliation.
        supervisor = SUPERVISOR_REGISTRY.get().publish_wal(database_oid);

        // Migration bridge: also wake the old database worker until the
        // ephemeral-maintenance phase replaces it completely.
        if let Some(worker_pid) = WAKE_REGISTRY
            .get()
            .publish(database_oid)
            .and_then(|wake| wake.worker_pid)
        {
            set_background_worker_latch(worker_pid.get(), Some(database_oid));
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

/// PostgreSQL 18 renamed `isBackgroundWorker` to `isRegularBackend` (inverted).
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
