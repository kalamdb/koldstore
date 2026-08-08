//! Cluster-wide KoldStore worker supervisor.
//!
//! This is the only permanent cluster coordinator. It keeps one lightweight,
//! latch-driven WAL applier available for each KoldStore-active database while
//! retaining ephemeral workers for maintenance and heavy flush execution.
//! Durable logical slots and jobs remain the source of truth; the supervisor
//! supplies low-latency dispatch, capacity control, and recovery.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use koldstore_common::unix_now_ms;
use koldstore_supervisor::{
    next_wait_duration, DatabaseWorkSnapshot, DynamicWorkerKind, RegistrationBackoff, LIBRARY_NAME,
    SAFETY_RECONCILE_INTERVAL, SUPERVISOR_REGISTRY_CAPACITY,
};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::datum::DatumWithOid;

const SUPERVISOR_FUNCTION: &str = "koldstore_supervisor_main";
const SUPERVISOR_NAME: &str = "koldstore supervisor";
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
    let mut registration_backoff = RegistrationBackoff::default();
    let mut flush_dispatch_cursor = 0_usize;
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
            super::wake::fill_supervisor_snapshots(&mut snapshots);
        }
        dispatch_shared_work(
            &snapshots,
            &mut registration_backoff,
            &mut flush_dispatch_cursor,
        );

        if super::wake::overflow_reconcile_required()
            || super::wal::overflow_reconcile_required()
            || (has_slots && last_safety.elapsed() >= SAFETY_RECONCILE_INTERVAL)
        {
            match super::txn::run(reconcile_cluster_safety) {
                Ok(value) => {
                    has_slots = value;
                    super::wake::clear_overflow_reconcile_required();
                    super::wal::clear_overflow_reconcile_required();
                }
                Err(error) => {
                    pgrx::warning!("koldstore supervisor safety reconciliation failed: {error}");
                }
            }
            last_safety = Instant::now();
        }

        super::wake::fill_supervisor_snapshots(&mut snapshots);
        let wait = next_wait_duration(
            has_slots,
            last_safety,
            &registration_backoff,
            lifecycle_reconcile_at,
            &snapshots,
            Instant::now(),
            unix_now_ms(),
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
    unsafe {
        if !pgrx::pg_sys::MyLatch.is_null() {
            pgrx::pg_sys::SetLatch(pgrx::pg_sys::MyLatch);
        }
        pgrx::pg_sys::procsignal_sigusr1_handler(signal);
    }
}

/// Dispatches published shared work without opening a PostgreSQL transaction.
fn dispatch_shared_work(
    snapshots: &[DatabaseWorkSnapshot],
    backoff: &mut RegistrationBackoff,
    flush_dispatch_cursor: &mut usize,
) {
    let now = Instant::now();

    // Required WAL services stay resident even while caught up. Dirty
    // generations only set the already-running process latch; process creation
    // is never part of steady-state commit latency.
    for snapshot in snapshots {
        let Some(wal_service) = super::wal::snapshot(snapshot.database_oid) else {
            backoff.clear_if_idle(snapshot.database_oid, DynamicWorkerKind::Wal);
            continue;
        };
        if !wal_service.required {
            backoff.clear_if_idle(snapshot.database_oid, DynamicWorkerKind::Wal);
            continue;
        }
        if super::wake::ensure_paused(snapshot.database_oid) {
            continue;
        }

        let recovery_requested =
            snapshot.event_flags & koldstore_supervisor::EVENT_RECOVERY_REQUIRED != 0;
        let wal_due =
            recovery_requested || snapshot.wal_generation != snapshot.wal_processed_generation;
        if wal_service.running() {
            // A stale PID must not block replacement of a required service,
            // including when the database is already caught up.
            if super::wal::ensure_live(snapshot.database_oid) {
                if wal_due {
                    let _ = super::wal::wake(snapshot.database_oid);
                }
                backoff.succeeded(snapshot.database_oid, DynamicWorkerKind::Wal);
                continue;
            }
        } else if wal_service.starting() {
            continue;
        }
        // Registration opens a new backend connected by OID. Probe first so a
        // dropped database cannot FATAL-loop and exhaust max_worker_processes.
        if retire_if_database_absent(snapshot.database_oid) {
            continue;
        }
        if !backoff.ready(snapshot.database_oid, DynamicWorkerKind::Wal, now) {
            continue;
        }
        if !super::wal::try_reserve(snapshot.database_oid) {
            continue;
        }
        match super::register_wal_applier_from_supervisor(snapshot.database_oid) {
            Ok(()) => backoff.succeeded(snapshot.database_oid, DynamicWorkerKind::Wal),
            Err(error) => {
                super::wal::cancel_start(snapshot.database_oid);
                let delay = backoff.failed(snapshot.database_oid, DynamicWorkerKind::Wal, now);
                pgrx::log!(
                    "koldstore supervisor: WAL applier registration deferred for db={} retry_in={}ms ({error})",
                    snapshot.database_oid,
                    delay.as_millis()
                );
            }
        }
    }

    // Maintenance is still burst/ephemeral and no longer owns normal WAL apply.
    for snapshot in snapshots {
        let maintenance_due =
            snapshot.maintenance_generation != snapshot.maintenance_processed_generation;
        if !maintenance_due {
            backoff.clear_if_idle(snapshot.database_oid, DynamicWorkerKind::Maintenance);
            continue;
        }
        if snapshot.maintenance_pid != 0
            || super::wake::ensure_paused(snapshot.database_oid)
            || !backoff.ready(snapshot.database_oid, DynamicWorkerKind::Maintenance, now)
        {
            continue;
        }
        if retire_if_database_absent(snapshot.database_oid) {
            continue;
        }
        if !super::wake::try_reserve_maintenance(snapshot.database_oid) {
            continue;
        }
        match super::register_maintenance_from_supervisor(snapshot.database_oid) {
            Ok(()) => backoff.succeeded(snapshot.database_oid, DynamicWorkerKind::Maintenance),
            Err(error) => {
                super::wake::cancel_maintenance_start(snapshot.database_oid);
                let delay =
                    backoff.failed(snapshot.database_oid, DynamicWorkerKind::Maintenance, now);
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

    let mut cluster_flush_workers = snapshots
        .iter()
        .map(|snapshot| snapshot.flush_workers())
        .fold(0_u32, u32::saturating_add);
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
            if retire_if_database_absent(snapshot.database_oid) {
                continue;
            }
            if !backoff.ready(snapshot.database_oid, DynamicWorkerKind::Flush, now) {
                continue;
            }

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
                    break;
                }
            }
        }
    }

    *flush_dispatch_cursor = (start + 1) % len;
}

/// Publishes generations for deadlines reached since the last wake.
fn publish_reached_deadlines(snapshots: &[DatabaseWorkSnapshot]) -> bool {
    let now_ms = unix_now_ms();
    let mut published = false;
    for snapshot in snapshots {
        let flush_deadline = snapshot.next_flush_due_at_ms;
        let flush_due = flush_deadline > 0 && flush_deadline <= now_ms;
        let maintenance_deadline = snapshot.next_maintenance_due_at_ms;
        let maintenance_due = maintenance_deadline > 0 && maintenance_deadline <= now_ms;
        if !(flush_due || maintenance_due) {
            continue;
        }
        // Deadlines for dropped databases must be retired, not republished —
        // otherwise dispatch spawn-crashes until the next safety reconcile.
        if retire_if_database_absent(snapshot.database_oid) {
            continue;
        }

        if flush_due && super::wake::consume_flush_deadline(snapshot.database_oid, flush_deadline) {
            super::wake::publish_due_flush(snapshot.database_oid);
            published = true;
        }

        if maintenance_due
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

fn reconcile_cluster_startup() -> Result<bool, String> {
    let slots = discover_async_slots()?;
    for (oid, _) in &slots {
        if !super::wal::require(*oid) {
            return Err(format!(
                "WAL applier registry is full while registering database {oid}"
            ));
        }
        super::wake::request_recovery(*oid);
    }
    Ok(!slots.is_empty())
}

/// Rare correctness safety pass.
fn reconcile_cluster_safety() -> Result<bool, String> {
    reconcile_worker_liveness()?;
    let slots = discover_async_slots()?;
    let slot_oids: HashSet<u32> = slots.iter().map(|(oid, _)| *oid).collect();
    for (oid, retained_bytes) in &slots {
        if !super::wal::require(*oid) {
            return Err(format!(
                "WAL applier registry is full while reconciling database {oid}"
            ));
        }
        if *retained_bytes > 0 {
            super::wake::request_recovery(*oid);
        }
    }

    // Capture teardown / DROP DATABASE leave registry entries behind. Without
    // retirement the supervisor spawn-crashes forever on missing OIDs and can
    // exhaust max_worker_processes for live databases (auto-flush starves).
    let mut snapshots = Vec::with_capacity(SUPERVISOR_REGISTRY_CAPACITY);
    super::wake::fill_supervisor_snapshots(&mut snapshots);
    for snapshot in &snapshots {
        if !slot_oids.contains(&snapshot.database_oid) {
            retire_absent_capture(snapshot.database_oid);
        }
    }
    let mut required_wal = Vec::with_capacity(SUPERVISOR_REGISTRY_CAPACITY);
    super::wal::required_oids_into(&mut required_wal);
    for oid in required_wal {
        if !slot_oids.contains(&oid) {
            retire_absent_capture(oid);
        }
    }

    Ok(!slots.is_empty())
}

/// Repairs worker reservations from PostgreSQL's authoritative process list.
fn reconcile_worker_liveness() -> Result<(), String> {
    let (wal_counts, maintenance_counts, flush_counts) = pgrx::Spi::connect(|client| {
        let table = client
            .select(
                "SELECT backend_type \
                 FROM pg_catalog.pg_stat_activity \
                 WHERE backend_type LIKE 'koldstore wal applier %' \
                    OR backend_type LIKE 'koldstore maintenance %' \
                    OR backend_type LIKE 'koldstore flush executor %'",
                None,
                &[],
            )
            .map_err(|error| error.to_string())?;
        let mut wal_counts = HashMap::<u32, u32>::new();
        let mut maintenance_counts = HashMap::<u32, u32>::new();
        let mut flush_counts = HashMap::<u32, u32>::new();
        for row in table {
            let backend_type = row
                .get::<String>(1)
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
            // Prefer the OID embedded in backend_type. `datid` can be NULL on
            // PG18 while a bgworker is between pgstat_beinit and pgstat_bestart;
            // treating that as "not live" falsely clears reservations and can
            // spawn duplicate WAL/maintenance/flush workers.
            let Some(database_oid) =
                koldstore_supervisor::database_oid_from_worker_backend_type(&backend_type)
            else {
                continue;
            };
            if backend_type.starts_with("koldstore wal applier ") {
                *wal_counts.entry(database_oid).or_default() += 1;
            } else if backend_type.starts_with("koldstore maintenance ") {
                *maintenance_counts.entry(database_oid).or_default() += 1;
            } else if backend_type.starts_with("koldstore flush executor ") {
                *flush_counts.entry(database_oid).or_default() += 1;
            }
        }
        Ok::<_, String>((wal_counts, maintenance_counts, flush_counts))
    })?;

    let mut snapshots = Vec::with_capacity(SUPERVISOR_REGISTRY_CAPACITY);
    super::wake::fill_supervisor_snapshots(&mut snapshots);
    for snapshot in snapshots {
        let wal_state = super::wal::snapshot(snapshot.database_oid);
        let wal_count = wal_counts.get(&snapshot.database_oid).copied().unwrap_or(0);
        let wal_stale = wal_state.is_some_and(|state| {
            if state.pid > 0 {
                // PGPROC is authoritative; activity can lag behind registration.
                !super::wal::process_alive(snapshot.database_oid, state.pid)
            } else if state.pid < 0 {
                wal_count == 0
            } else {
                false
            }
        });
        if wal_stale {
            super::wal::clear_stale(snapshot.database_oid);
            if wal_state.is_some_and(|state| state.required) {
                if database_oid_exists_spi(snapshot.database_oid)? {
                    super::wake::request_recovery(snapshot.database_oid);
                } else {
                    retire_absent_capture(snapshot.database_oid);
                }
            }
        }
        if wal_count > 1 {
            pgrx::warning!(
                "koldstore supervisor: multiple WAL appliers for db={} count={wal_count}",
                snapshot.database_oid
            );
        }

        let maintenance_count = maintenance_counts
            .get(&snapshot.database_oid)
            .copied()
            .unwrap_or(0);
        let maintenance_stale = if snapshot.maintenance_pid > 0 {
            !super::proc_latch::background_worker_alive(
                snapshot.maintenance_pid,
                Some(snapshot.database_oid),
            )
        } else if snapshot.maintenance_pid < 0 {
            maintenance_count == 0
        } else {
            false
        };
        if maintenance_stale {
            super::wake::clear_stale_maintenance(snapshot.database_oid);
            if database_oid_exists_spi(snapshot.database_oid)? {
                super::wake::request_recovery(snapshot.database_oid);
            } else {
                // Stale maintenance for a dropped DB used to request_recovery,
                // which re-armed dispatch and spawned FATAL workers in a loop.
                retire_absent_capture(snapshot.database_oid);
            }
        }
        if maintenance_count > 1 {
            pgrx::warning!(
                "koldstore supervisor: multiple maintenance workers for db={} count={maintenance_count}",
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
                if database_oid_exists_spi(snapshot.database_oid)? {
                    super::wake::request_recovery(snapshot.database_oid);
                } else {
                    retire_absent_capture(snapshot.database_oid);
                }
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

/// Retires supervisor + WAL dispatch for a database that no longer has capture.
fn retire_absent_capture(database_oid: u32) {
    super::wal::disable(database_oid);
    super::wal::clear_stale(database_oid);
    super::wake::quiesce_database(database_oid);
    pgrx::log!("koldstore supervisor: retired absent capture db={database_oid}");
}

fn database_oid_exists_spi(database_oid: u32) -> Result<bool, String> {
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE oid = $1::oid)",
        &[DatumWithOid::from(pgrx::pg_sys::Oid::from(database_oid))],
    )
    .map_err(|error| error.to_string())
    .map(|value| value.unwrap_or(false))
}

/// Existence probe for the latch-driven supervisor loop (no open transaction).
///
/// `SearchSysCache` asserts `IsTransactionState()`, so probes must open a short
/// SPI transaction. Call only when about to register a worker or publish a
/// reached deadline — not on every idle wake of an already-running service.
fn database_present_for_dispatch(database_oid: u32) -> bool {
    match super::txn::run(|| database_oid_exists_spi(database_oid)) {
        Ok(exists) => exists,
        Err(error) => {
            pgrx::log!(
                "koldstore supervisor: database existence probe deferred for db={database_oid}: {error}"
            );
            // Fail open: a transient probe error must not retire a live database.
            true
        }
    }
}

/// Retires capture when the database OID is gone. Returns `true` when retired.
fn retire_if_database_absent(database_oid: u32) -> bool {
    if database_present_for_dispatch(database_oid) {
        return false;
    }
    retire_absent_capture(database_oid);
    true
}
