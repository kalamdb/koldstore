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
const REGISTRATION_RETRY_MIN: Duration = Duration::from_millis(100);
const REGISTRATION_RETRY_MAX: Duration = Duration::from_secs(5);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DynamicWorkerKind {
    Maintenance,
    Flush,
}

#[derive(Debug, Clone, Copy)]
struct RegistrationRetry {
    failures: u8,
    next_attempt_at: Instant,
}

/// Supervisor-local retry state for dynamic-worker registration pressure.
///
/// `max_worker_processes` exhaustion is not durable work failure, so retrying is
/// correct, but a fixed 250ms cluster wake burns CPU indefinitely under sustained
/// pressure. Backoff lives only in the static supervisor; durable generations
/// remain dirty and survive supervisor/postmaster restart independently.
#[derive(Debug, Default)]
struct RegistrationBackoff {
    entries: HashMap<(u32, DynamicWorkerKind), RegistrationRetry>,
}

impl RegistrationBackoff {
    fn ready(&self, database_oid: u32, kind: DynamicWorkerKind, now: Instant) -> bool {
        self.entries
            .get(&(database_oid, kind))
            .is_none_or(|retry| now >= retry.next_attempt_at)
    }

    fn succeeded(&mut self, database_oid: u32, kind: DynamicWorkerKind) {
        self.entries.remove(&(database_oid, kind));
    }

    fn failed(&mut self, database_oid: u32, kind: DynamicWorkerKind, now: Instant) -> Duration {
        let failures = self
            .entries
            .get(&(database_oid, kind))
            .map(|retry| retry.failures.saturating_add(1))
            .unwrap_or(1)
            .min(16);
        let shift = u32::from(failures.saturating_sub(1)).min(6);
        let multiplier = 1_u32 << shift;
        let delay = REGISTRATION_RETRY_MIN
            .saturating_mul(multiplier)
            .min(REGISTRATION_RETRY_MAX);
        self.entries.insert(
            (database_oid, kind),
            RegistrationRetry {
                failures,
                next_attempt_at: now + delay,
            },
        );
        delay
    }

    fn clear_if_idle(&mut self, database_oid: u32, kind: DynamicWorkerKind) {
        self.entries.remove(&(database_oid, kind));
    }

    fn next_wait(&self, now: Instant) -> Option<Duration> {
        self.entries
            .values()
            .map(|retry| retry.next_attempt_at.saturating_duration_since(now))
            .min()
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
    let mut registration_backoff = RegistrationBackoff::default();
    let mut flush_dispatch_cursor = 0_usize;
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
        if publish_reached_deadlines(&snapshots) {
            // Deadline publication mutates generations. Refresh before dispatch
            // so due maintenance/flush work starts in this same iteration.
            super::wake::fill_supervisor_snapshots(&mut snapshots);
        }
        dispatch_shared_work(
            &snapshots,
            &mut registration_backoff,
            &mut flush_dispatch_cursor,
        );

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
            &registration_backoff,
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
    // bgw_notify_pid delivers SIGUSR1 on child lifecycle changes. Explicitly
    // set the process latch before delegating so an infinite WaitLatch wakes even
    // if the generic ProcSignal handler has no reason to set it for this signal.
    unsafe {
        if !pgrx::pg_sys::MyLatch.is_null() {
            pgrx::pg_sys::SetLatch(pgrx::pg_sys::MyLatch);
        }
        pgrx::pg_sys::procsignal_sigusr1_handler(signal);
    }
}

/// Dispatches published shared work without opening a PostgreSQL transaction.
///
/// Maintenance registration is single-owner. Flush registration fills every
/// currently available per-database/cluster slot in fair rounds, so a burst of
/// queued jobs does not require one supervisor wake per executor.
fn dispatch_shared_work(
    snapshots: &[DatabaseWorkSnapshot],
    backoff: &mut RegistrationBackoff,
    flush_dispatch_cursor: &mut usize,
) {
    let now = Instant::now();

    for snapshot in snapshots {
        if !snapshot.maintenance_due() {
            backoff.clear_if_idle(snapshot.database_oid, DynamicWorkerKind::Maintenance);
            continue;
        }
        if snapshot.maintenance_pid != 0
            || super::wake::ensure_paused(snapshot.database_oid)
            || !backoff.ready(snapshot.database_oid, DynamicWorkerKind::Maintenance, now)
        {
            continue;
        }
        if !super::wake::try_reserve_maintenance(snapshot.database_oid) {
            continue;
        }
        match super::register_maintenance_from_supervisor(snapshot.database_oid) {
            Ok(()) => backoff.succeeded(snapshot.database_oid, DynamicWorkerKind::Maintenance),
            Err(error) => {
                super::wake::cancel_maintenance_start(snapshot.database_oid);
                let delay = backoff.failed(
                    snapshot.database_oid,
                    DynamicWorkerKind::Maintenance,
                    now,
                );
                pgrx::log!(
                    "koldstore supervisor: maintenance registration deferred for db={} retry_in={}ms ({error})",
                    snapshot.database_oid,
                    delay.as_millis()
                );
            }
        }
    }

    if snapshots.is_empty() {
        *flush_dispatch_cursor = 0;
        return;
    }

    let mut cluster_flush_workers = super::wake::flush_workers_total();
    if cluster_flush_workers >= CLUSTER_FLUSH_WORKER_LIMIT {
        return;
    }

    let len = snapshots.len();
    let start = *flush_dispatch_cursor % len;
    let mut made_progress = true;
    while made_progress && cluster_flush_workers < CLUSTER_FLUSH_WORKER_LIMIT {
        made_progress = false;
        for offset in 0..len {
            if cluster_flush_workers >= CLUSTER_FLUSH_WORKER_LIMIT {
                break;
            }
            let snapshot = snapshots[(start + offset) % len];
            if !snapshot.flush_due() {
                backoff.clear_if_idle(snapshot.database_oid, DynamicWorkerKind::Flush);
                continue;
            }
            if !backoff.ready(snapshot.database_oid, DynamicWorkerKind::Flush, now) {
                continue;
            }

            // Refresh this one lock-free entry because earlier rounds in this
            // same dispatch pass may already have reserved workers for it.
            let Some(current) = super::wake::supervisor_snapshot(snapshot.database_oid) else {
                continue;
            };
            if !current.flush_due() || current.flush_workers() >= current.flush_limit {
                continue;
            }
            if !super::wake::try_reserve_flush(snapshot.database_oid) {
                continue;
            }

            cluster_flush_workers = cluster_flush_workers.saturating_add(1);
            match super::register_flush_executor_from_supervisor(snapshot.database_oid) {
                Ok(()) => {
                    backoff.succeeded(snapshot.database_oid, DynamicWorkerKind::Flush);
                    made_progress = true;
                }
                Err(error) => {
                    super::wake::cancel_flush_start(snapshot.database_oid);
                    cluster_flush_workers = cluster_flush_workers.saturating_sub(1);
                    let delay =
                        backoff.failed(snapshot.database_oid, DynamicWorkerKind::Flush, now);
                    pgrx::log!(
                        "koldstore supervisor: flush registration deferred for db={} retry_in={}ms ({error})",
                        snapshot.database_oid,
                        delay.as_millis()
                    );
                    // Registration pressure is process-wide in practice. Do not
                    // hammer every remaining database in this round after one
                    // load_dynamic failure; wait for a child exit/backoff wake.
                    break;
                }
            }
        }
    }

    // Rotate first-served database across dispatches so a cluster cap smaller
    // than the number of busy databases cannot permanently favor low registry
    // slots. Additional workers are still assigned one-per-database per round.
    *flush_dispatch_cursor = (start + 1) % len;
}

/// Publishes generations for deadlines reached since the last wake.
///
/// Queue retry deadlines dispatch flush executors directly. Timed auto-flush
/// policies publish maintenance work so eligibility is re-evaluated without a
/// permanently running per-database worker.
fn publish_reached_deadlines(snapshots: &[DatabaseWorkSnapshot]) -> bool {
    let now_ms = unix_now_ms();
    let mut published = false;
    for snapshot in snapshots {
        let flush_deadline = snapshot.next_flush_due_at_ms;
        if flush_deadline > 0
            && flush_deadline <= now_ms
            && super::wake::consume_flush_deadline(snapshot.database_oid, flush_deadline)
        {
            super::wake::publish_due_flush(snapshot.database_oid);
            published = true;
        }

        let maintenance_deadline = snapshot.next_maintenance_due_at_ms;
        if maintenance_deadline > 0
            && maintenance_deadline <= now_ms
            && super::wake::consume_maintenance_deadline(
                snapshot.database_oid,
                maintenance_deadline,
            )
        {
            super::wake::publish_due_maintenance(snapshot.database_oid);
            published = true;
        }
    }
    published
}

fn next_wait_duration(
    has_slots: bool,
    last_safety: Instant,
    registration_backoff: &RegistrationBackoff,
    lifecycle_reconcile_at: Option<Instant>,
    snapshots: &[DatabaseWorkSnapshot],
) -> Option<Duration> {
    let now = Instant::now();
    let mut wait = registration_backoff.next_wait(now);
    if has_slots {
        wait = min_optional_duration(
            wait,
            Some(SAFETY_RECONCILE_INTERVAL.saturating_sub(last_safety.elapsed())),
        );
    }
    if let Some(deadline) = lifecycle_reconcile_at {
        wait = min_optional_duration(wait, Some(deadline.saturating_duration_since(now)));
    }

    let now_ms = unix_now_ms();
    for snapshot in snapshots {
        wait = min_optional_duration(wait, deadline_delay(snapshot.next_flush_due_at_ms, now_ms));
        wait = min_optional_duration(
            wait,
            deadline_delay(snapshot.next_maintenance_due_at_ms, now_ms),
        );
    }

    // None means an infinite latch wait: when no slot/deadline/retry exists,
    // KoldStore has zero polling wakeups.
    wait.map(|duration| duration.max(Duration::from_millis(1)))
}

fn deadline_delay(deadline_ms: i64, now_ms: i64) -> Option<Duration> {
    if deadline_ms <= 0 {
        return None;
    }
    let delay_ms = deadline_ms.saturating_sub(now_ms).max(1);
    Some(Duration::from_millis(
        u64::try_from(delay_ms).unwrap_or(u64::MAX),
    ))
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

#[derive(Debug, Clone, Copy)]
struct MaintenanceLiveness {
    first_pid: i32,
    count: u32,
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
        let mut maintenance = HashMap::<u32, MaintenanceLiveness>::new();
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
                maintenance
                    .entry(database_oid)
                    .and_modify(|state| state.count = state.count.saturating_add(1))
                    .or_insert(MaintenanceLiveness {
                        first_pid: pid,
                        count: 1,
                    });
            } else if backend_type.starts_with("koldstore flush executor ") {
                *flush_counts.entry(database_oid).or_default() += 1;
            }
        }
        Ok::<_, String>((maintenance, flush_counts))
    })?;

    let mut snapshots = Vec::with_capacity(SUPERVISOR_REGISTRY_CAPACITY);
    super::wake::fill_supervisor_snapshots(&mut snapshots);
    for snapshot in snapshots {
        let live_maintenance = maintenance.get(&snapshot.database_oid).copied();
        let maintenance_stale = if snapshot.maintenance_pid > 0 {
            live_maintenance.is_none_or(|state| state.first_pid != snapshot.maintenance_pid)
        } else if snapshot.maintenance_pid < 0 {
            live_maintenance.is_none()
        } else {
            false
        };
        if maintenance_stale {
            super::wake::clear_stale_maintenance(snapshot.database_oid);
            super::wake::request_recovery(snapshot.database_oid);
        }
        if live_maintenance.is_some_and(|state| state.count > 1) {
            pgrx::warning!(
                "koldstore supervisor: multiple maintenance workers for db={} count={}",
                snapshot.database_oid,
                live_maintenance.map(|state| state.count).unwrap_or(0)
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
