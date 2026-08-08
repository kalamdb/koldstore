//! Persistent per-database WAL-applier background worker.
//!
//! WAL application is a latency-sensitive service, not scheduled maintenance.
//! One worker stays registered for each database that owns a KoldStore logical
//! slot, sleeps on a latch with a long recovery watchdog, drains committed WAL
//! through the existing fixed-fence protocol, and returns to sleep. Heavy flush
//! and maintenance work remains in separate ephemeral processes.

use std::time::Duration;

use koldstore_wal::{
    wal_applier_worker_type, WalApplierRegistry, WalApplierSnapshot, WAL_APPLIER_REGISTRY_CAPACITY,
};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::{pg_guard, pg_shmem_init, pg_sys, AssertPGRXSharedMemory, PgAtomic};

use crate::mirror::apply::{apply_bounded, capture_durable_wal_fence, BoundedApplyRequest};

const WAL_APPLIER_FUNCTION: &str = "koldstore_wal_applier_main";
const WAL_APPLIER_WATCHDOG: Duration = Duration::from_secs(30);

type SharedWalApplierRegistry =
    AssertPGRXSharedMemory<WalApplierRegistry<WAL_APPLIER_REGISTRY_CAPACITY>>;

static WAL_APPLIER_REGISTRY: PgAtomic<SharedWalApplierRegistry> =
    unsafe { PgAtomic::new(c"koldstore wal applier registry") };

#[allow(unexpected_cfgs)]
pub(crate) fn initialize() {
    pg_shmem_init!(
        WAL_APPLIER_REGISTRY =
            unsafe { AssertPGRXSharedMemory::new(WalApplierRegistry::default()) }
    );
}

#[must_use]
pub(crate) fn require(database_oid: u32) -> bool {
    WAL_APPLIER_REGISTRY.get().require(database_oid)
}

pub(crate) fn disable(database_oid: u32) {
    WAL_APPLIER_REGISTRY.get().disable(database_oid);
}

#[must_use]
pub(crate) fn snapshot(database_oid: u32) -> Option<WalApplierSnapshot> {
    WAL_APPLIER_REGISTRY.get().snapshot(database_oid)
}

pub(crate) fn try_reserve(database_oid: u32) -> bool {
    WAL_APPLIER_REGISTRY.get().try_reserve(database_oid)
}

pub(crate) fn cancel_start(database_oid: u32) {
    WAL_APPLIER_REGISTRY.get().cancel_start(database_oid);
}

pub(crate) fn clear_stale(database_oid: u32) {
    WAL_APPLIER_REGISTRY.get().clear_stale(database_oid);
}

#[must_use]
pub(crate) fn overflow_reconcile_required() -> bool {
    WAL_APPLIER_REGISTRY.get().overflow_reconcile_required()
}

pub(crate) fn clear_overflow_reconcile_required() {
    WAL_APPLIER_REGISTRY
        .get()
        .clear_overflow_reconcile_required();
}

/// Wakes an already-running applier. Returns false when it must be started.
#[must_use]
pub(crate) fn wake(database_oid: u32) -> bool {
    let Some(state) = snapshot(database_oid) else {
        return false;
    };
    if !state.required || state.pid <= 0 {
        return false;
    }
    set_background_worker_latch(state.pid, database_oid)
}

/// Registers one already-reserved persistent WAL applier.
///
/// The static cluster supervisor is the sole production caller. Registration
/// remains dynamic so only databases with an actual KoldStore logical slot use
/// a PostgreSQL worker slot.
pub(crate) fn register_from_supervisor(database_oid: u32) -> Result<(), String> {
    let worker_type = wal_applier_worker_type(database_oid);
    BackgroundWorkerBuilder::new(&worker_type)
        .set_type(&worker_type)
        .set_library(koldstore_worker::LIBRARY_NAME)
        .set_function(WAL_APPLIER_FUNCTION)
        .enable_spi_access()
        .set_restart_time(None)
        .set_argument(Some(pg_sys::Datum::from(database_oid)))
        .set_notify_pid(unsafe { pg_sys::MyProcPid })
        .load_dynamic()
        .map(|_| ())
        .map_err(|_| {
            format!(
                "could not register WAL applier (worker_type={worker_type}; \
                 usually max_worker_processes exhausted)"
            )
        })
}

#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_wal_applier_main(argument: pg_sys::Datum) {
    run_wal_applier(argument.value() as u32);
}

fn run_wal_applier(database_oid: u32) {
    attach_signal_handlers();
    BackgroundWorker::connect_worker_to_spi_by_oid(Some(pg_sys::Oid::from(database_oid)), None);

    if !WAL_APPLIER_REGISTRY
        .get()
        .started(database_oid, unsafe { pg_sys::MyProcPid })
    {
        pgrx::log!("koldstore WAL applier db={database_oid}: stale/unreserved start; exiting");
        return;
    }
    let _registration = WalApplierRegistration { database_oid };
    // Recovery and scheduling share one durable generation. Apply at most once
    // for a given recovery generation; the ephemeral maintenance worker may
    // clear the flag later without making this process spin on an unchanged bit.
    let mut applied_recovery_generation = 0_u64;

    loop {
        let Some(service) = snapshot(database_oid) else {
            return;
        };
        if !service.required {
            return;
        }

        let slot = crate::mirror::lifecycle::slot_name(database_oid);
        if !crate::mirror::lifecycle::native_slot_exists(&slot) {
            return;
        }

        let Some(state) = super::wake::supervisor_snapshot(database_oid) else {
            return;
        };
        let target_generation = state.wal_generation;
        let recovery_requested = state.event_flags & koldstore_worker::EVENT_RECOVERY_REQUIRED != 0;
        let recovery_apply_due =
            recovery_requested && state.maintenance_generation > applied_recovery_generation;
        let wal_due = state.wal_generation != state.wal_processed_generation;

        if wal_due || recovery_apply_due {
            if let Err(error) = drain_wal_through_fixed_fence() {
                crate::observability::record_async_apply_error();
                pgrx::warning!("koldstore WAL applier db={database_oid} apply deferred: {error}");
                super::wake::request_recovery(database_oid);
                return;
            }
            if wal_due {
                super::wake::mark_wal_processed(database_oid, target_generation);
            }
            if recovery_apply_due {
                applied_recovery_generation = state.maintenance_generation;
            }
            // A commit or recovery request that arrived during this pass changed
            // a generation. Re-read shared state immediately instead of sleeping.
            continue;
        }

        // Latch wakes are latency hints. The long timeout is only a recovery
        // watchdog for COMMIT PREPARED or a missed in-memory signal; there is no
        // normal polling transaction while the worker is idle.
        if !BackgroundWorker::wait_latch(Some(WAL_APPLIER_WATCHDOG)) {
            return;
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP) };
        }
    }
}

/// Drains all WAL visible at one fixed durable fence while respecting bounded
/// apply budgets. `synchronous_commit=off` stays foreground-asynchronous: this
/// worker performs XLogFlush, not the application backend.
fn drain_wal_through_fixed_fence() -> Result<(), String> {
    let fence = capture_durable_wal_fence()?;
    loop {
        let decoding_log_guard = DecodingLogGuard::suppress_routine_log_messages();
        let outcome = super::txn::run_recoverable("WAL applier", || {
            let mut request = BoundedApplyRequest::available();
            request.upper_bound = Some(fence);
            request.advance_slot_on_empty = true;
            apply_bounded(request)
        });
        drop(decoding_log_guard);
        let outcome = outcome?;
        crate::observability::record_async_apply_tick(outcome.row_changes, 0);
        if !outcome.budget_exhausted {
            return Ok(());
        }
    }
}

struct WalApplierRegistration {
    database_oid: u32,
}

impl Drop for WalApplierRegistration {
    fn drop(&mut self) {
        WAL_APPLIER_REGISTRY
            .get()
            .stopped(self.database_oid, unsafe { pg_sys::MyProcPid });
        super::wake::wake_supervisor();
    }
}

struct DecodingLogGuard {
    previous: std::os::raw::c_int,
}

impl DecodingLogGuard {
    fn suppress_routine_log_messages() -> Self {
        unsafe {
            let previous = pg_sys::log_min_messages;
            pg_sys::log_min_messages = pg_sys::FATAL as std::os::raw::c_int;
            Self { previous }
        }
    }
}

impl Drop for DecodingLogGuard {
    fn drop(&mut self) {
        unsafe {
            pg_sys::log_min_messages = self.previous;
        }
    }
}

fn attach_signal_handlers() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP);
    unsafe {
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        pg_sys::pqsignal(pg_sys::SIGTERM as i32, Some(wal_applier_sigterm));
        #[cfg(feature = "pg18")]
        pg_sys::pqsignal_be(pg_sys::SIGTERM as i32, Some(wal_applier_sigterm));
    }
}

unsafe extern "C-unwind" fn wal_applier_sigterm(signal: std::os::raw::c_int) {
    unsafe { pg_sys::die(signal) }
}

fn set_background_worker_latch(pid: i32, database_oid: u32) -> bool {
    unsafe {
        let process = pg_sys::BackendPidGetProc(pid);
        if process.is_null()
            || (*process).pid != pid
            || !is_background_worker(process)
            || (*process).databaseId.to_u32() != database_oid
        {
            return false;
        }
        pg_sys::SetLatch(&raw mut (*process).procLatch);
        true
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
