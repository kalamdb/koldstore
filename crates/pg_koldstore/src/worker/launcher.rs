//! Cluster-wide KoldStore worker supervisor.
//!
//! This is the only permanent KoldStore maintenance process. It sleeps on a
//! PostgreSQL latch, owns all dynamic worker registration, and performs only a
//! rare cluster safety reconciliation. Normal WAL/flush work is event driven;
//! durable logical slots and jobs remain the source of truth.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use koldstore_worker::LIBRARY_NAME;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};

const SUPERVISOR_FUNCTION: &str = "koldstore_async_mirror_launcher_main";
const SUPERVISOR_NAME: &str = "koldstore supervisor";
/// Rare retained-WAL/process-liveness safety net, not a normal scheduler tick.
const SAFETY_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
/// Avoid spawning a DB worker merely for a handful of unrelated WAL records.
const SAFETY_WAL_LAG_BYTES: i64 = 16 * 1024 * 1024;
/// Retry only after RegisterDynamicBackgroundWorker itself reports pressure.
const DISPATCH_RETRY: Duration = Duration::from_millis(250);
/// Conservative cluster-wide cap until a dedicated cluster GUC lands.
const CLUSTER_FLUSH_WORKER_LIMIT: u32 = 8;

/// PostgreSQL sends `bgw_notify_pid` SIGUSR1 when a dynamic child starts/exits.
/// Its normal procsignal handler sets MyLatch; this flag tells the main loop that
/// the wake specifically requires an authoritative child-liveness reconciliation.
static CHILD_LIFECYCLE_PENDING: AtomicBool = AtomicBool::new(false);

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

#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_async_mirror_launcher_main(_argument: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    install_sigusr1_lifecycle_handler();
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);
    let _registration = SupervisorRegistration::new();

    // Shared memory is reconstructed after postmaster restart. Recover worker
    // reservations and seed every durable KoldStore slot exactly once at startup.
    let mut has_slots = match super::txn::run(reconcile_cluster_startup) {
        Ok(has_slots) => has_slots,
        Err(error) => {
            pgrx::warning!("koldstore supervisor startup reconciliation failed: {error}");
            true
        }
    };
    let mut last_safety = Instant::now();

    loop {
        if CHILD_LIFECYCLE_PENDING.swap(false, Ordering::AcqRel) {
            if let Err(error) = super::txn::run(reconcile_worker_liveness) {
                pgrx::log!("koldstore supervisor child reconciliation deferred: {error}");
            }
        }

        publish_reached_queue_deadlines();
        let registration_pressure = dispatch_shared_work();

        if super::wake::overflow_reconcile_required()
            || (has_slots && last_safety.elapsed() >= SAFETY_RECONCILE_INTERVAL)
        {
            match super::txn::run(reconcile_cluster_safety) {
                Ok(still_has_slots) => {
                    has_slots = still_has_slots;
                    super::wake::clear_overflow_reconcile_required();
                }
                Err(error) => {
                    pgrx::warning!("koldstore supervisor safety reconciliation failed: {error}");
                }
            }
            last_safety = Instant::now();
        }

        let wait = next_wait_duration(has_slots, last_safety, registration_pressure);
        if !BackgroundWorker::wait_latch(wait) {
            // A bgworker that exits 0 is not restarted. Exit non-zero so the
            // postmaster's bgw_restart_time remains our only permanent supervisor.
            unsafe { pgrx::pg_sys::proc_exit(1) };
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
    }
}

fn install_sigusr1_lifecycle_handler() {
    unsafe {
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        pgrx::pg_sys::pqsignal(
            pgrx::pg_sys::SIGUSR1 as i32,
            Some(supervisor_sigusr1_handler),
        );
        #[cfg(feature = "pg18")]
        pgrx::pg_sys::pqsignal_be(
            pgrx::pg_sys::SIGUSR1 as i32,
            Some(supervisor_sigusr1_handler),
        );
    }
}

unsafe extern "C-unwind" fn supervisor_sigusr1_handler(signal: std::os::raw::c_int) {
    CHILD_LIFECYCLE_PENDING.store(true, Ordering::Release);
    // Preserve PostgreSQL's ProcSignal processing and, importantly, SetLatch(MyLatch).
    unsafe { pgrx::pg_sys::procsignal_sigusr1_handler(signal) }
}

/// Dispatches already-published shared work without opening a transaction.
fn dispatch_shared_work() -> bool {
    let mut registration_pressure = false;
    for snapshot in super::wake::supervisor_snapshots() {
        if snapshot.maintenance_due()
            && snapshot.maintenance_pid == 0
            && !super::wake::ensure_paused(snapshot.database_oid)
            && super::wake::try_reserve_maintenance(snapshot.database_oid)
        {
            if let Err(error) =
                super::register_maintenance_from_supervisor(snapshot.database_oid)
            {
                super::wake::cancel_maintenance_start(snapshot.database_oid);
                registration_pressure = true;
                pgrx::log!(
                    "koldstore supervisor: maintenance registration deferred for db={} ({error})",
                    snapshot.database_oid
                );
            }
        }

        if !snapshot.flush_due() || snapshot.flush_workers() >= snapshot.flush_limit {
            continue;
        }
        if !super::wake::try_reserve_flush(snapshot.database_oid, CLUSTER_FLUSH_WORKER_LIMIT) {
            continue;
        }
        if let Err(error) = super::register_flush_executor_from_supervisor(snapshot.database_oid) {
            super::wake::cancel_flush_start(snapshot.database_oid);
            registration_pressure = true;
            pgrx::log!(
                "koldstore supervisor: flush registration deferred for db={} ({error})",
                snapshot.database_oid
            );
        }
    }
    registration_pressure
}

/// Converts exact future queue deadlines into normal flush generations.
fn publish_reached_queue_deadlines() {
    let now_ms = unix_now_ms();
    for snapshot in super::wake::supervisor_snapshots() {
        let deadline = snapshot.next_flush_due_at_ms;
        if deadline <= 0 || deadline > now_ms {
            continue;
        }
        if super::wake::consume_flush_deadline(snapshot.database_oid, deadline) {
            super::wake::publish_due_flush(snapshot.database_oid);
        }
    }
}

fn next_wait_duration(
    has_slots: bool,
    last_safety: Instant,
    registration_pressure: bool,
) -> Option<Duration> {
    let mut wait = registration_pressure.then_some(DISPATCH_RETRY);

    if has_slots {
        wait = min_optional_duration(
            wait,
            Some(SAFETY_RECONCILE_INTERVAL.saturating_sub(last_safety.elapsed())),
        );
    }

    let now_ms = unix_now_ms();
    for snapshot in super::wake::supervisor_snapshots() {
        let deadline = snapshot.next_flush_due_at_ms;
        if deadline <= 0 {
            continue;
        }
        let delay_ms = deadline.saturating_sub(now_ms).max(1);
        wait = min_optional_duration(
            wait,
            Some(Duration::from_millis(
                u64::try_from(delay_ms).unwrap_or(u64::MAX),
            )),
        );
    }

    // None intentionally means WaitLatch without a timeout: zero polling when
    // there are no logical slots, retries, or real queue deadlines.
    wait.map(|duration| duration.max(Duration::from_millis(1)))
}

fn min_optional_duration(left: Option<Duration>, right: Option<Duration>) -> Option<Duration> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

/// Startup is authoritative because shared hints disappear on postmaster restart.
fn reconcile_cluster_startup() -> Result<bool, String> {
    reconcile_worker_liveness()?;
    let slots = discover_async_slots()?;
    for (oid, _) in &slots {
        super::wake::request_recovery(*oid);
    }
    Ok(!slots.is_empty())
}

/// Rare safety pass: repair dead dynamic workers and prevent unrelated WAL from
/// accumulating forever. A DB worker is launched only after the slot gap becomes
/// meaningful; managed commits themselves use immediate generation wakeups.
fn reconcile_cluster_safety() -> Result<bool, String> {
    reconcile_worker_liveness()?;
    let slots = discover_async_slots()?;
    for (oid, retained_bytes) in &slots {
        if *retained_bytes >= SAFETY_WAL_LAG_BYTES {
            super::wake::request_recovery(*oid);
        }
    }
    Ok(!slots.is_empty())
}

/// Repairs shared worker reservations from PostgreSQL's authoritative process list.
/// This runs at startup, on native bgw_notify_pid SIGUSR1, and on the rare safety pass.
fn reconcile_worker_liveness() -> Result<(), String> {
    let (maintenance, flush_counts) = pgrx::Spi::connect(|client| {
        let table = client
            .select(
                "SELECT datid::oid, pid::int4, backend_type \
                 FROM pg_catalog.pg_stat_activity \
                 WHERE backend_type LIKE 'koldstore async mirror %' \
                    OR backend_type LIKE 'koldstore flush executor %'",
                None,
                &[],
            )
            .map_err(|error| error.to_string())?;
        let mut maintenance = HashMap::<u32, Vec<i32>>::new();
        let mut flush_counts = HashMap::<u32, u32>::new();
        for row in table {
            let Some(datid) = row
                .get::<pgrx::pg_sys::Oid>(1)
                .map_err(|error| error.to_string())?
            else {
                continue;
            };
            let pid = row
                .get::<i32>(2)
                .map_err(|error| error.to_string())?
                .unwrap_or(0);
            let backend_type = row
                .get::<String>(3)
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
            let database_oid = datid.to_u32();
            if backend_type.starts_with("koldstore async mirror ") {
                maintenance.entry(database_oid).or_default().push(pid);
            } else if backend_type.starts_with("koldstore flush executor ") {
                *flush_counts.entry(database_oid).or_default() += 1;
            }
        }
        Ok::<_, String>((maintenance, flush_counts))
    })?;

    for snapshot in super::wake::supervisor_snapshots() {
        let live_maintenance = maintenance
            .get(&snapshot.database_oid)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let maintenance_stale = if snapshot.maintenance_pid > 0 {
            !live_maintenance.contains(&snapshot.maintenance_pid)
        } else if snapshot.maintenance_pid < 0 {
            live_maintenance.is_empty()
        } else {
            false
        };
        if maintenance_stale {
            super::wake::clear_stale_maintenance(snapshot.database_oid);
            super::wake::request_recovery(snapshot.database_oid);
        }
        if live_maintenance.len() > 1 {
            pgrx::warning!(
                "koldstore supervisor: multiple maintenance workers observed for db={} pids={live_maintenance:?}",
                snapshot.database_oid
            );
        }

        let actual_flush = flush_counts
            .get(&snapshot.database_oid)
            .copied()
            .unwrap_or(0);
        if snapshot.flush_workers() != actual_flush {
            let lost_owner = snapshot.flush_workers() > actual_flush;
            super::wake::reconcile_flush_counts(snapshot.database_oid, actual_flush);
            if lost_owner {
                // A SIGKILL/FATAL executor cannot run Rust Drop. DB recovery will
                // reclaim its same durable running job under the table lock.
                super::wake::request_recovery(snapshot.database_oid);
            }
        }
    }
    Ok(())
}

/// Returns every KoldStore logical slot and its current retained WAL gap.
fn discover_async_slots() -> Result<Vec<(u32, i64)>, String> {
    pgrx::Spi::connect(|client| {
        let table = client
            .select(
                "SELECT d.oid::oid, \
                        CASE WHEN s.confirmed_flush_lsn IS NULL THEN 9223372036854775807::bigint \
                             ELSE GREATEST(pg_catalog.pg_wal_lsn_diff(\
                                    pg_catalog.pg_current_wal_lsn(), s.confirmed_flush_lsn\
                                  )::bigint, 0) \
                        END AS retained_bytes \
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
            let Some(oid) = row
                .get::<pgrx::pg_sys::Oid>(1)
                .map_err(|error| error.to_string())?
            else {
                continue;
            };
            let retained = row
                .get::<i64>(2)
                .map_err(|error| error.to_string())?
                .unwrap_or(0);
            out.push((oid.to_u32(), retained));
        }
        Ok(out)
    })
}

fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
