//! Cluster launcher that keeps async mirror appliers registered after boot.
//!
//! Registered from `_PG_init` only while `shared_preload_libraries` loads
//! `koldstore`. Without preload, the first backend transaction that sees an
//! async slot still calls ensure (see [`super::ensure`]).

use std::time::Duration;

use koldstore_worker::{DatabaseOid, LAUNCHER_POLL_INTERVAL_MS, LIBRARY_NAME};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};

const LAUNCHER_FUNCTION: &str = "koldstore_async_mirror_launcher_main";
const LAUNCHER_NAME: &str = "koldstore async mirror launcher";

/// Registers the static launcher when the extension is shared-preloaded.
pub(crate) fn register_if_shared_preload() {
    let preloading = unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress };
    if !preloading {
        return;
    }
    BackgroundWorkerBuilder::new(LAUNCHER_NAME)
        .set_type(LAUNCHER_NAME)
        .set_library(LIBRARY_NAME)
        .set_function(LAUNCHER_FUNCTION)
        .enable_spi_access()
        // Static preload launcher may restart; dynamic ensure() path uses NEVER_RESTART.
        .set_restart_time(Some(Duration::from_secs(1)))
        .load();
}

/// Discovers async logical slots and ensures one applier per database.
#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_async_mirror_launcher_main(_argument: pgrx::pg_sys::Datum) {
    // SIGHUP + SIGTERM so postmaster stop does not leave an unhandled signal as
    // a non-zero exit that ensure() then respawns in a tight loop.
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    // Connect to the default database so we can read cluster-wide slot catalogs.
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);
    let poll = Duration::from_millis(LAUNCHER_POLL_INTERVAL_MS);
    loop {
        if let Err(error) = worker_transaction(ensure_appliers_for_async_slots) {
            pgrx::log!("koldstore async mirror launcher: ensure failed: {error}");
        }
        if !BackgroundWorker::wait_latch(Some(poll)) {
            break;
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
    }
}

fn ensure_appliers_for_async_slots() -> Result<(), String> {
    let oids = pgrx::Spi::connect(|client| -> Result<Vec<u32>, String> {
        let table = client
            .select(
                "SELECT d.oid::oid \
                 FROM pg_catalog.pg_replication_slots s \
                 JOIN pg_catalog.pg_database d ON d.datname = s.database \
                 WHERE s.slot_name LIKE 'koldstore_async_%' \
                   AND s.slot_type = 'logical' \
                   AND s.plugin = 'pgoutput'",
                None,
                &[],
            )
            .map_err(|error| error.to_string())?;
        let mut out = Vec::new();
        for row in table {
            if let Some(oid) = row
                .get::<pgrx::pg_sys::Oid>(1)
                .map_err(|error| error.to_string())?
            {
                out.push(oid.to_u32());
            }
        }
        Ok(out)
    })?;
    for oid in oids {
        // Skip when a session ensure already holds the registration lock
        // (e.g. manage_table's open transaction). Blocking here wedged the
        // launcher under pg_test and led to ensure() respawn storms.
        if !crate::async_mirror::lifecycle::try_lock_worker_registration(oid)? {
            continue;
        }
        let _ = super::ensure::ensure_async_mirror_worker_for(DatabaseOid::new(oid));
    }
    Ok(())
}

fn worker_transaction<R>(body: impl FnOnce() -> Result<R, String>) -> Result<R, String> {
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
