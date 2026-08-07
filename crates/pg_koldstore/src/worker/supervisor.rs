//! Cluster-wide KoldStore worker supervisor.
//!
//! This is the only permanent KoldStore maintenance process. Normal WAL/flush
//! work is event driven. Durable logical slots and jobs remain the source of
//! truth; the supervisor only supplies low-latency dispatch and recovery.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use koldstore_worker::{DatabaseWorkSnapshot, LIBRARY_NAME, SUPERVISOR_REGISTRY_CAPACITY};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};

const SUPERVISOR_FUNCTION: &str = "koldstore_supervisor_main";
const SUPERVISOR_NAME: &str = "koldstore supervisor";
const SAFETY_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
const DISPATCH_RETRY: Duration = Duration::from_millis(250);
const CHILD_LIFECYCLE_GRACE: Duration = Duration::from_secs(1);
const CLUSTER_FLUSH_WORKER_LIMIT: u32 = 8;

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
pub extern "C-unwind" fn koldstore_supervisor_main(_argument: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    install_sigusr1_lifecycle_handler();
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);
    let _registration = SupervisorRegistration::new();

    let mut has_slots = match super::txn::run(reconcile_cluster_startup) {
        Ok(value) => value,
        Err(error) => {
            pgrx::warning!("koldstore supervisor startup reconciliation failed: {error}");
            true
        }
    };
    let mut last_safety = Instant::now();
    let mut lifecycle_reconcile_at = Some(Instant::now() + CHILD_LIFECYCLE_GRACE);
    // Permanent process: allocate the registry view once and reuse it for every
    // wake/deadline calculation instead of creating several short-lived Vecs.
    let mut snapshots = Vec::with_capacity(SUPERVISOR_REGISTRY_CAPACITY);

    loop {
        if CHILD_LIFECYCLE_PENDING.swap(false, Ordering::AcqRel) {
            // Start and exit use the same SIGUSR1 notification. Give a newly
            // started worker a short grace to publish its PID/activity state.
            lifecycle_reconcile_at = Some(Instant::now() + CHILD_LIFECYCLE_GRACE);
        }

        if lifecycle_reconcile_at.is_some_and(|deadline| Instant::now() >= deadline) {
            if let Err(error) = super::txn::run(reconcile_worker_liveness) {
                pgrx::log!("koldstore supervisor child reconciliation deferred: {error}");
                lifecycle_reconcile_at = Some(Instant::now() + CHILD_LIFECYCLE_GRACE);
            } else {
                lifecycle_reconcile_at = None;
            }
        }

        super::wake::fill_supervisor_snapshots(&mut snapshots);
        if publish_reached_queue_deadlines(&snapshots) {
            // Deadline publication mutates flush generations. Refresh before
            // dispatch so due work can start in this same supervisor iteration.
            super::wake::fill_supervisor_snapshots(&mut snapshots);
        }
        let registration_pressure = dispatch_shared_work(&snapshots);

        if super::wake::overflow_reconcile_required()
            || (has_slots && last_safety.elapsed() >= SAFETY_RECONCILE_INTERVAL)
        {
            match super::txn::run(reconcile_cluster_safety) {
                Ok(value) => {
                    has_slots = value;
                    super::wake::clear_overflow_reconcile_required();
                }
                Err(error) => {
                    pgrx::warning!("koldstore supervisor safety reconciliation failed: {error}");
                }
            }
            last_safety = Instant::now();
        }

        // Dispatch/reconciliation may have changed reservations/deadlines. Reuse
        // the same allocation and take one fresh coherent-ish snapshot for sleep.
        super::wake::fill_supervisor_snapshots(&mut snapshots);
        let wait = next_wait_duration(
            has_slots,
            last_safety,
            registration_pressure,
            lifecycle_reconcile_at,
            &snapshots,
        );
        if !BackgroundWorker::wait_latch(wait) {
            // bgworker exit 0 disables restart; non-zero preserves the static
            // supervisor's postmaster bgw_restart_time contract.
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
    // Preserve PostgreSQL's normal ProcSignal processing and MyLatch wakeup.
    unsafe { pgrx::pg_sys::procsignal_sigusr1_handler(signal) }
}

/// Dispatches published shared work without opening a PostgreSQL transaction.
fn dispatch_shared_work(snapshots: &[DatabaseWorkSnapshot]) -> bool {
    let mut registration_pressure = false;
    // Cluster capacity is sampled once. The supervisor is the only process that
    // reserves new flush workers, so a local counter avoids an O(N) registry scan
    // for each database while still counting live/start-in-progress workers.
    let mut cluster_flush_workers = super::wake::flush_workers_total();

    for snapshot in snapshots {
        if snapshot.maintenance_due()
            && snapshot.maintenance_pid == 0
            && !super::wake::ensure_paused(snapshot.database_oid)
            && super::wake::try_reserve_maintenance(snapshot.database_oid)
        {
            if let Err(error) = super::register_maintenance_from_supervisor(snapshot.database_oid) {
                super::wake::cancel_maintenance_start(snapshot.database_oid);
                registration_pressure = true;
                pgrx::log!(
                    "koldstore supervisor: maintenance registration deferred for db={} ({error})",
                    snapshot.database_oid
                );
            }
        }

        if !snapshot.flush_due()
            || snapshot.flush_workers() >= snapshot.flush_limit
            || cluster_flush_workers >= CLUSTER_FLUSH_WORKER_LIMIT
        {
            continue;
        }
        if !super::wake::try_reserve_flush(snapshot.database_oid) {
            continue;
        }
        cluster_flush_workers = cluster_flush_workers.saturating_add(1);
        if let Err(error) = super::register_flush_executor_from_supervisor(snapshot.database_oid) {
            super::wake::cancel_flush_start(snapshot.database_oid);
            cluster_flush_workers = cluster_flush_workers.saturating_sub(1);
            registration_pressure = true;
            pgrx::log!(
                "koldstore supervisor: flush registration deferred for db={} ({error})",
                snapshot.database_oid
            );
        }
    }
    registration_pressure
}

/// Publishes flush generations for queue deadlines reached since the last wake.
/// Returns true when shared state changed and the caller should refresh snapshots.
fn publish_reached_queue_deadlines(snapshots: &[DatabaseWorkSnapshot]) -> bool {
    let now_ms = unix_now_ms();
    let mut published = false;
    for snapshot in snapshots {
        let deadline = snapshot.next_flush_due_at_ms;
        if deadline > 0
            && deadline <= now_ms
            && super::wake::consume_flush_deadline(snapshot.database_oid, deadline)
        {
            super::wake::publish_due_flush(snapshot.database_oid);
            published = true;
        }
    }
    published
}

fn next_wait_duration(
    has_slots: bool,
    last_safety: Instant,
    registration_pressure: bool,
    lifecycle_reconcile_at: Option<Instant>,
    snapshots: &[DatabaseWorkSnapshot],
) -> Option<Duration> {
    let mut wait = registration_pressure.then_some(DISPATCH_RETRY);
    if has_slots {
        wait = min_optional_duration(
            wait,
            Some(SAFETY_RECONCILE_INTERVAL.saturating_sub(last_safety.elapsed())),
        );
    }
    if let Some(deadline) = lifecycle_reconcile_at {
        wait = min_optional_duration(
            wait,
            Some(deadline.saturating_duration_since(Instant::now())),
        );
    }

    let now_ms = unix_now_ms();
    for snapshot in snapshots {
        if snapshot.next_flush_due_at_ms <= 0 {
            continue;
        }
        let delay_ms = snapshot.next_flush_due_at_ms.saturating_sub(now_ms).max(1);
        wait = min_optional_duration(
            wait,
            Some(Duration::from_millis(
                u64::try_from(delay_ms).unwrap_or(u64::MAX),
            )),
        );
    }

    // None means an infinite latch wait: when no slot/deadline/retry exists,
    // KoldStore has zero polling wakeups.
    wait.map(|duration| duration.max(Duration::from_millis(1)))
}

fn min_optional_duration(left: Option<Duration>, right: Option<Duration>) -> Option<Duration> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn reconcile_cluster_startup() -> Result<bool, String> {
    let slots = discover_async_slots()?;
    for (oid, _) in &slots {
        super::wake::request_recovery(*oid);
    }
    Ok(!slots.is_empty())
}

/// Rare correctness safety pass.
///
/// Any positive retained-WAL gap is enough to request one bounded maintenance
/// pass. This is deliberately conservative: it covers COMMIT PREPARED, unusual
/// indirect WAL, and missed in-memory hints without keeping a per-DB worker
/// alive. Ordinary managed commits remain immediate through shared generations.
fn reconcile_cluster_safety() -> Result<bool, String> {
    reconcile_worker_liveness()?;
    let slots = discover_async_slots()?;
    for (oid, retained_bytes) in &slots {
        if *retained_bytes > 0 {
            super::wake::request_recovery(*oid);
        }
    }
    Ok(!slots.is_empty())
}

/// Repairs worker reservations from PostgreSQL's authoritative process list.
/// This SQL path runs only after child lifecycle events/startup/safety checks.
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

    let mut snapshots = Vec::with_capacity(SUPERVISOR_REGISTRY_CAPACITY);
    super::wake::fill_supervisor_snapshots(&mut snapshots);
    for snapshot in snapshots {
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
                "koldstore supervisor: multiple maintenance workers for db={} pids={live_maintenance:?}",
                snapshot.database_oid
            );
        }

        let actual_flush = flush_counts
            .get(&snapshot.database_oid)
            .copied()
            .unwrap_or(0);
        if snapshot.flush_workers() != actual_flush {
            let lost_owner = snapshot.flush_running > actual_flush;
            super::wake::reconcile_flush_counts(snapshot.database_oid, actual_flush);
            if lost_owner {
                super::wake::request_recovery(snapshot.database_oid);
            }
        }
    }
    Ok(())
}

fn discover_async_slots() -> Result<Vec<(u32, i64)>, String> {
    pgrx::Spi::connect(|client| {
        let table = client
            .select(
                "SELECT d.oid::oid, \
                        CASE WHEN s.confirmed_flush_lsn IS NULL \
                             THEN 9223372036854775807::bigint \
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
