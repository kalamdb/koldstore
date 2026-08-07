//! Cluster-wide KoldStore worker supervisor.
//!
//! This is the only permanent KoldStore maintenance process.  It sleeps on a
//! PostgreSQL latch, owns dynamic flush-worker registration, and performs only a
//! rare safety reconciliation.  Normal queue work is event driven: a committed
//! job advances a shared generation and sets this process's latch.
//!
//! The legacy per-database WAL applier is still re-ensured during the rare
//! safety pass while its execution is migrated to an ephemeral maintenance
//! worker.  Importantly, the old 500ms catalog polling loop is gone.

use std::time::{Duration, Instant};

use koldstore_worker::{DatabaseOid, LIBRARY_NAME};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};

const SUPERVISOR_FUNCTION: &str = "koldstore_async_mirror_launcher_main";
const SUPERVISOR_NAME: &str = "koldstore supervisor";
/// Correctness safety net, not a normal scheduling cadence.
const SAFETY_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
/// Resource-pressure retry while durable queue work is waiting for a worker slot.
const DISPATCH_RETRY: Duration = Duration::from_millis(250);
/// Conservative cluster-wide cap until a dedicated GUC is introduced.
const CLUSTER_FLUSH_WORKER_LIMIT: u32 = 8;

/// Registers the static postmaster-supervised process from shared_preload.
pub(crate) fn register_if_shared_preload() {
    let preloading = unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress };
    if !preloading {
        return;
    }
    BackgroundWorkerBuilder::new(SUPERVISOR_NAME)
        .set_type(SUPERVISOR_NAME)
        .set_library(LIBRARY_NAME)
        .set_function(SUPERVISOR_FUNCTION)
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(1)))
        .load();
}

struct SupervisorRegistration;

impl SupervisorRegistration {
    fn new() -> Self {
        super::wake::register_supervisor();
        Self
    }
}

impl Drop for SupervisorRegistration {
    fn drop(&mut self) {
        super::wake::unregister_supervisor();
    }
}

/// Static supervisor entry point.
#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_async_mirror_launcher_main(_argument: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    // Cluster-wide slot discovery is available from the postgres database.
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);
    let _registration = SupervisorRegistration::new();

    // One authoritative startup reconciliation reconstructs in-memory hints
    // after postmaster restart. Durable slots/jobs remain the source of truth.
    if let Err(error) = super::txn::run(reconcile_cluster_safety) {
        pgrx::warning!("koldstore supervisor startup reconciliation failed: {error}");
    }

    let mut last_safety = Instant::now();
    loop {
        let pressure = dispatch_shared_work();

        if last_safety.elapsed() >= SAFETY_RECONCILE_INTERVAL
            || super::wake::overflow_reconcile_required()
        {
            if let Err(error) = super::txn::run(reconcile_cluster_safety) {
                pgrx::warning!("koldstore supervisor safety reconciliation failed: {error}");
            } else {
                super::wake::clear_overflow_reconcile_required();
            }
            last_safety = Instant::now();
        }

        let safety_wait = SAFETY_RECONCILE_INTERVAL.saturating_sub(last_safety.elapsed());
        let wait = if pressure {
            safety_wait.min(DISPATCH_RETRY)
        } else {
            safety_wait
        };
        if !BackgroundWorker::wait_latch(Some(wait.max(Duration::from_millis(1)))) {
            // Exit non-zero so bgw_restart_time relaunches after an admin
            // terminate. During postmaster shutdown PostgreSQL suppresses restart.
            unsafe { pgrx::pg_sys::proc_exit(1) };
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
    }
}

/// Dispatches already-published shared work without opening a PostgreSQL transaction.
///
/// Returns true when durable work exists but capacity/registration pressure
/// requires a short retry rather than the normal safety deadline.
fn dispatch_shared_work() -> bool {
    let mut pressure = false;
    for snapshot in super::wake::supervisor_snapshots() {
        if !snapshot.flush_due() {
            continue;
        }
        if !super::wake::try_reserve_flush(snapshot.database_oid, CLUSTER_FLUSH_WORKER_LIMIT) {
            pressure = true;
            continue;
        }
        if let Err(error) =
            super::register_flush_executor_from_supervisor(snapshot.database_oid)
        {
            super::wake::cancel_flush_start(snapshot.database_oid);
            pressure = true;
            pgrx::log!(
                "koldstore supervisor: flush worker registration deferred for db={} ({error})",
                snapshot.database_oid
            );
        }
    }
    pressure
}

/// Rare authoritative recovery pass.
///
/// For now this retains legacy WAL-applier recovery while that worker is moved
/// to the same ephemeral supervisor model. It is intentionally *not* a 500 ms
/// scheduling loop. Each discovered slot also seeds shared recovery state so a
/// lost in-memory wake cannot become permanent.
fn reconcile_cluster_safety() -> Result<(), String> {
    let oids = discover_async_slot_databases()?;
    for oid in oids {
        super::wake::request_recovery(oid);

        // Migration bridge for WAL apply. Once the ephemeral DB maintenance
        // worker lands, this ensure call and ensure.rs are removed completely.
        if crate::worker::wake::ensure_paused(oid) {
            continue;
        }
        super::ensure::mark_worker_not_ensured();
        if let Err(error) = super::ensure::ensure_async_mirror_worker_for(DatabaseOid::new(oid)) {
            pgrx::log!(
                "koldstore supervisor: legacy WAL applier ensure deferred for db={oid}: {error}"
            );
        }
    }
    Ok(())
}

fn discover_async_slot_databases() -> Result<Vec<u32>, String> {
    pgrx::Spi::connect(|client| -> Result<Vec<u32>, String> {
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
    })
}
