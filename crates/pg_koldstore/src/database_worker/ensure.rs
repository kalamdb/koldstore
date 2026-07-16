//! Ensure/register the async mirror database worker via pgrx.

use std::sync::atomic::{AtomicBool, Ordering};

use koldstore_worker::{
    async_mirror_worker_type, ensure_action, DatabaseOid, EnsureAction, LIBRARY_NAME,
};
use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::datum::DatumWithOid;

const APPLIER_FUNCTION: &str = "koldstore_async_mirror_applier_main";
const LAUNCHER_FUNCTION: &str = "koldstore_async_mirror_launcher_main";
const LAUNCHER_NAME: &str = "koldstore async mirror launcher";

/// Per-backend latch: the first query may re-register after postmaster restart.
static WORKER_ENSURED: AtomicBool = AtomicBool::new(false);

/// Clears the current backend's worker fast path after explicit cleanup.
pub(crate) fn mark_worker_not_ensured() {
    WORKER_ENSURED.store(false, Ordering::Relaxed);
}

fn worker_running(worker_type: &str) -> Result<bool, String> {
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE backend_type = $1)",
        &[DatumWithOid::from(worker_type)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "database worker activity query returned no row".to_string())
}

fn async_slot_exists_for_current_database() -> Result<bool, String> {
    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    let slot = crate::async_mirror::lifecycle::slot_name(database_oid);
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (\
           SELECT 1 FROM pg_catalog.pg_replication_slots \
           WHERE slot_name = $1 AND slot_type = 'logical' AND plugin = 'pgoutput'\
         )",
        &[DatumWithOid::from(slot.as_str())],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "async slot probe returned no row".to_string())
}

/// Ensures the cluster launcher is running (auto-restarts on its own crashes).
fn ensure_launcher() -> Result<(), String> {
    if worker_running(LAUNCHER_NAME)? {
        return Ok(());
    }
    let worker = BackgroundWorkerBuilder::new(LAUNCHER_NAME)
        .set_type(LAUNCHER_NAME)
        .set_library(LIBRARY_NAME)
        .set_function(LAUNCHER_FUNCTION)
        .enable_spi_access()
        // Never restart: postmaster stop must not fight a respawning launcher.
        // ensure_async_mirror_worker() re-registers the launcher when needed.
        .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
        .load_dynamic()
        .map_err(|_| "could not register the async mirror launcher".to_string())?;
    worker
        .wait_for_startup()
        .map_err(|status| format!("async mirror launcher did not start: {status:?}"))?;
    Ok(())
}

/// Ensures one persistent WAL applier is running for the current database.
///
/// Appliers use `BGW_NEVER_RESTART` so dropping the slot can leave them stopped.
/// The launcher (and session ensure) re-registers them after crashes.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot inspect or start the worker.
pub(crate) fn ensure_async_mirror_worker() -> Result<bool, String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Ok(false);
    }
    ensure_launcher()?;
    let database_oid = DatabaseOid::new(unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32());
    ensure_async_mirror_worker_for(database_oid)
}

/// Ensures the applier for `database_oid` (used by the boot launcher).
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot inspect or start the worker.
pub(crate) fn ensure_async_mirror_worker_for(database_oid: DatabaseOid) -> Result<bool, String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Ok(false);
    }

    let worker_type = async_mirror_worker_type(database_oid);
    let running = worker_running(&worker_type)?;
    match ensure_action(running) {
        EnsureAction::AlreadyRunning => {
            WORKER_ENSURED.store(true, Ordering::Relaxed);
            return Ok(false);
        }
        EnsureAction::Register => {
            WORKER_ENSURED.store(false, Ordering::Relaxed);
        }
    }

    crate::async_mirror::lifecycle::lock_worker_registration(database_oid.get())?;
    if worker_running(&worker_type)? {
        WORKER_ENSURED.store(true, Ordering::Relaxed);
        return Ok(false);
    }

    // Never restart: intentional slot drop must leave the applier stopped.
    // Crash recovery is the launcher's job (poll + re-register).
    let worker = BackgroundWorkerBuilder::new(&worker_type)
        .set_type(&worker_type)
        .set_library(LIBRARY_NAME)
        .set_function(APPLIER_FUNCTION)
        .enable_spi_access()
        .set_argument(Some(pgrx::pg_sys::Datum::from(database_oid.get())))
        .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
        .load_dynamic()
        .map_err(|_| "could not register the async mirror WAL applier".to_string())?;
    worker
        .wait_for_startup()
        .map_err(|status| format!("async mirror WAL applier did not start: {status:?}"))?;
    WORKER_ENSURED.store(true, Ordering::Relaxed);
    Ok(true)
}

/// Once per backend, starts the applier when this database still has an async slot.
///
/// Covers postmaster restart without requiring per-DML triggers.
pub(crate) fn ensure_async_mirror_worker_once_if_needed() {
    if WORKER_ENSURED.load(Ordering::Relaxed) {
        return;
    }
    if !crate::guc::async_mirror_worker_enabled() {
        WORKER_ENSURED.store(true, Ordering::Relaxed);
        return;
    }
    let exists = match async_slot_exists_for_current_database() {
        Ok(exists) => exists,
        Err(_) => return,
    };
    if !exists {
        WORKER_ENSURED.store(true, Ordering::Relaxed);
        return;
    }
    let _ = ensure_async_mirror_worker();
}

/// Ensures async capture starts a database worker before activation completes.
///
/// # Errors
///
/// Returns an error when the worker GUC is disabled or registration fails.
pub(crate) fn require_async_mirror_worker() -> Result<(), String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Err(
            "async mirror capture requires koldstore.internal_async_mirror_worker=on".to_string(),
        );
    }
    ensure_async_mirror_worker()?;
    Ok(())
}

/// Internal SQL entry point for diagnostics and tests.
///
/// SQL contract: ensures the current database worker is running and returns
/// whether this call registered it. Delegates to [`ensure_async_mirror_worker`].
#[pgrx::pg_extern(
    name = "internal_ensure_async_mirror_worker",
    schema = "koldstore",
    security_definer
)]
pub fn ensure_async_mirror_worker_pg() -> bool {
    ensure_async_mirror_worker()
        .unwrap_or_else(|error| pgrx::error!("could not start async mirror worker: {error}"))
}
