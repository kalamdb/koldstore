//! Ensure/register the async mirror database worker via pgrx.

use std::sync::atomic::{AtomicBool, Ordering};

use koldstore_worker::{
    async_mirror_worker_type, ensure_action, DatabaseOid, EnsureAction, LIBRARY_NAME,
};
use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::datum::DatumWithOid;

const APPLIER_FUNCTION: &str = "koldstore_async_mirror_applier_main";

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
    let slot = crate::mirror::lifecycle::slot_name(database_oid);
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

/// Ensures one persistent WAL applier is running for the current database.
///
/// Appliers use `BGW_NEVER_RESTART` so dropping the slot can leave them stopped.
/// Soft SPI errors stay in-process with backoff; hard process death is recovered
/// by the shared-preload launcher (sub-second poll) or by session ensure
/// (manage / fences / SQL). The launcher is registered only from shared_preload
/// so a crashing launcher cannot starve worker slots under `cargo pgrx test`.
/// The same worker also runs auto-flush scheduling.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot inspect or start the worker.
pub(crate) fn ensure_async_mirror_worker() -> Result<bool, String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Ok(false);
    }
    let database_oid = DatabaseOid::new(unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32());
    ensure_async_mirror_worker_for(database_oid)
}

/// Ensures the applier for `database_oid` (used by the boot launcher).
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot inspect or start the worker.
pub(crate) fn ensure_async_mirror_worker_for(database_oid: DatabaseOid) -> Result<bool, String> {
    // Shared-memory pause blocks both the launcher (on `postgres`) and session
    // ensure — advisory locks cannot do that across databases.
    if crate::worker::wake::ensure_paused(database_oid.get()) {
        return Ok(false);
    }
    if !crate::guc::async_mirror_worker_enabled() {
        return Ok(false);
    }

    let worker_type = async_mirror_worker_type(database_oid);
    // Per-backend latch: after this backend registers (or observes) the applier,
    // further ensure calls are no-ops until the worker exits. The worker may
    // still be connecting until the registering transaction commits, so an
    // open XID means "not visible yet" rather than "dead".
    //
    // Check for an assigned XID *before* any SPI: `worker_running` uses SPI and
    // would assign an XID, which falsely trips the in-xact guard and prevents
    // re-registration after a terminated NEVER_RESTART applier.
    if WORKER_ENSURED.load(Ordering::Relaxed) {
        let in_xact = unsafe { pgrx::pg_sys::GetCurrentTransactionIdIfAny() }
            != pgrx::pg_sys::InvalidTransactionId;
        if in_xact {
            return Ok(false);
        }
        if worker_running(&worker_type)? {
            return Ok(false);
        }
        WORKER_ENSURED.store(false, Ordering::Relaxed);
    }

    let running = worker_running(&worker_type)?;
    match ensure_action(running) {
        EnsureAction::AlreadyRunning => {
            WORKER_ENSURED.store(true, Ordering::Relaxed);
            return Ok(false);
        }
        EnsureAction::Register => {}
    }

    crate::mirror::lifecycle::lock_worker_registration(database_oid.get())?;
    if worker_running(&worker_type)? {
        WORKER_ENSURED.store(true, Ordering::Relaxed);
        return Ok(false);
    }

    // Never restart via postmaster: intentional slot drop must leave the applier
    // stopped. Crash recovery is the launcher's job (shared_preload) or a new
    // backend's ensure after mark_worker_not_ensured.
    //
    // Only wait for postmaster startup notification. The worker finishes
    // `BackgroundWorkerInitializeConnection` after this transaction commits, so
    // requiring `pg_stat_activity` visibility inside `manage_table` deadlocks
    // (worker waits for commit; we wait for the worker).
    let worker = BackgroundWorkerBuilder::new(&worker_type)
        .set_type(&worker_type)
        .set_library(LIBRARY_NAME)
        .set_function(APPLIER_FUNCTION)
        .enable_spi_access()
        .set_argument(Some(pgrx::pg_sys::Datum::from(database_oid.get())))
        .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
        .load_dynamic()
        .map_err(|_| {
            format!(
                "could not register the async mirror WAL applier \
                 (worker_type={worker_type}; usually max_worker_processes exhausted)"
            )
        })?;
    worker
        .wait_for_startup()
        .map_err(|status| format!("async mirror WAL applier did not start: {status:?}"))?;
    WORKER_ENSURED.store(true, Ordering::Relaxed);
    Ok(true)
}

/// Once per backend, starts the database worker when this database needs it.
///
/// Starts when an async slot exists or any active managed table has auto-flush
/// enabled. Covers postmaster restart without requiring per-DML triggers.
pub(crate) fn ensure_async_mirror_worker_once_if_needed() {
    if WORKER_ENSURED.load(Ordering::Relaxed) {
        return;
    }
    if !crate::guc::async_mirror_worker_enabled() {
        WORKER_ENSURED.store(true, Ordering::Relaxed);
        return;
    }
    let needs_worker = match needs_database_worker_for_current_database() {
        Ok(needs) => needs,
        Err(_) => return,
    };
    if !needs_worker {
        WORKER_ENSURED.store(true, Ordering::Relaxed);
        return;
    }
    let _ = ensure_async_mirror_worker();
}

fn needs_database_worker_for_current_database() -> Result<bool, String> {
    if async_slot_exists_for_current_database()? {
        return Ok(true);
    }
    // Extension may not be installed yet in this database.
    let has_schemas =
        pgrx::Spi::get_one::<bool>("SELECT to_regclass('koldstore.schemas') IS NOT NULL")
            .map_err(|error| error.to_string())?
            .unwrap_or(false);
    if !has_schemas {
        return Ok(false);
    }
    super::flush_task::database_has_auto_flush_tables()
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

/// Pauses or resumes ensure/register for the current database (cluster-wide).
///
/// SQL contract: `koldstore.internal_set_async_mirror_ensure_paused(paused)`.
/// When `paused` is true, the shared-preload launcher and session ensure skip
/// registration so tests can keep the NEVER_RESTART applier stopped. Returns
/// whether the pause set accepted the request (`false` only if the set is full).
#[pgrx::pg_extern(
    name = "internal_set_async_mirror_ensure_paused",
    schema = "koldstore",
    security_definer
)]
pub fn set_async_mirror_ensure_paused_pg(paused: bool) -> bool {
    let oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    if paused {
        if !crate::worker::wake::pause_ensure(oid) {
            pgrx::error!("async mirror ensure pause set is full");
        }
        mark_worker_not_ensured();
        true
    } else {
        crate::worker::wake::resume_ensure(oid);
        mark_worker_not_ensured();
        true
    }
}
