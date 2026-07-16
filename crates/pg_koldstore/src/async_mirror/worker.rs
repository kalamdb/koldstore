//! Database-scoped workers for async mirror provisioning and WAL apply.
//!
//! Logical slots cannot be created in the transaction that manages a table, so
//! a short-lived worker owns slot creation. A persistent worker then applies
//! committed WAL at a bounded polling interval without adding work to the
//! source transaction.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{ffi::CString, ptr};

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::datum::DatumWithOid;

use super::lifecycle::slot_name;

const LIBRARY_NAME: &str = "koldstore";
const PROVISIONER_FUNCTION: &str = "koldstore_async_mirror_slot_provisioner_main";
const APPLIER_FUNCTION: &str = "koldstore_async_mirror_applier_main";
const APPLY_INTERVAL: Duration = Duration::from_millis(100);
const RESTART_INTERVAL: Duration = Duration::from_secs(1);

// Each PostgreSQL backend only needs to discover or launch the database worker
// once. A crashed applier is restarted by the postmaster using its registration.
static APPLIER_ENSURED: AtomicBool = AtomicBool::new(false);

/// Clears the current backend's worker fast path after explicit cleanup.
pub(super) fn mark_applier_not_ensured() {
    APPLIER_ENSURED.store(false, Ordering::Relaxed);
}

fn applier_type(database_oid: u32) -> String {
    format!("koldstore async mirror {database_oid}")
}

fn applier_running(worker_type: &str) -> Result<bool, String> {
    pgrx::Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE backend_type = $1)",
        &[DatumWithOid::from(worker_type)],
    )
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "async mirror worker activity query returned no row".to_string())
}

/// Creates missing async infrastructure in an autonomous transaction.
///
/// The caller waits for worker shutdown and then validates the resulting slot,
/// so worker startup failures become a synchronous `manage_table` error.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot register, start, or stop the worker.
pub(super) fn provision_infrastructure(database_oid: u32) -> Result<(), String> {
    let worker = BackgroundWorkerBuilder::new("koldstore async slot provisioner")
        .set_type("koldstore async slot provisioner")
        .set_library(LIBRARY_NAME)
        .set_function(PROVISIONER_FUNCTION)
        .enable_spi_access()
        .set_argument(Some(pgrx::pg_sys::Datum::from(database_oid)))
        .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
        .load_dynamic()
        .map_err(|_| "could not register the async mirror slot provisioner".to_string())?;
    worker
        .wait_for_startup()
        .map_err(|status| format!("async mirror slot provisioner did not start: {status:?}"))?;
    worker
        .wait_for_shutdown()
        .map_err(|status| format!("async mirror slot provisioner did not stop: {status:?}"))
}

/// Ensures one persistent WAL applier is running for the current database.
///
/// The check is safe to call from the lightweight statement trigger installed
/// for async tables. A backend-local fast path avoids repeated catalog queries.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot inspect or start the worker.
pub(super) fn ensure_applier() -> Result<bool, String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Ok(false);
    }

    let database_oid = unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32();
    let worker_type = applier_type(database_oid);
    if APPLIER_ENSURED.load(Ordering::Relaxed) && applier_running(&worker_type)? {
        return Ok(false);
    }
    APPLIER_ENSURED.store(false, Ordering::Relaxed);

    super::lifecycle::lock_worker_registration(database_oid)?;
    if applier_running(&worker_type)? {
        APPLIER_ENSURED.store(true, Ordering::Relaxed);
        return Ok(false);
    }

    let worker = BackgroundWorkerBuilder::new(&worker_type)
        .set_type(&worker_type)
        .set_library(LIBRARY_NAME)
        .set_function(APPLIER_FUNCTION)
        .enable_spi_access()
        .set_restart_time(Some(RESTART_INTERVAL))
        .set_argument(Some(pgrx::pg_sys::Datum::from(database_oid)))
        .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
        .load_dynamic()
        .map_err(|_| "could not register the async mirror WAL applier".to_string())?;
    worker
        .wait_for_startup()
        .map_err(|status| format!("async mirror WAL applier did not start: {status:?}"))?;
    APPLIER_ENSURED.store(true, Ordering::Relaxed);
    Ok(true)
}

/// Ensures async capture has a live database worker before activation completes.
///
/// # Errors
///
/// Returns an error when the worker GUC is disabled or the applier cannot be
/// registered and observed in `pg_stat_activity`.
pub(super) fn require_applier_for_async_capture() -> Result<(), String> {
    if !crate::guc::async_mirror_worker_enabled() {
        return Err(
            "async mirror capture requires koldstore.internal_async_mirror_worker=on".to_string(),
        );
    }
    ensure_applier()?;
    let worker_type = applier_type(unsafe { pgrx::pg_sys::MyDatabaseId }.to_u32());
    if applier_running(&worker_type)? {
        Ok(())
    } else {
        Err("async mirror WAL applier is not running".to_string())
    }
}

/// Internal SQL entry point used by the async statement trigger.
///
/// SQL contract: ensures the current database worker is running and returns
/// whether this call registered it.
#[pgrx::pg_extern(
    name = "internal_ensure_async_mirror_worker",
    schema = "koldstore",
    security_definer
)]
pub fn ensure_applier_pg() -> bool {
    ensure_applier()
        .unwrap_or_else(|error| pgrx::error!("could not start async mirror worker: {error}"))
}

/// Autonomous worker entry point for deterministic slot creation.
#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_async_mirror_slot_provisioner_main(
    argument: pgrx::pg_sys::Datum,
) {
    let database_oid = argument.value() as u32;
    BackgroundWorker::connect_worker_to_spi_by_oid(
        Some(pgrx::pg_sys::Oid::from(database_oid)),
        None,
    );
    // SPI assigns a top-level XID in a connected background worker, and
    // PostgreSQL refuses logical-slot creation after that point. Use the same
    // native replication sequence as pg_create_logical_replication_slot.
    unsafe {
        pgrx::pg_sys::SetCurrentStatementStartTimestamp();
        pgrx::pg_sys::StartTransactionCommand();
        pgrx::pg_sys::PushActiveSnapshot(pgrx::pg_sys::GetTransactionSnapshot());
    }
    let slot = slot_name(database_oid);
    if !super::lifecycle::native_slot_exists(&slot) {
        create_logical_slot(&slot);
    }
    let publication_exists = pgrx::Spi::get_one::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = 'koldstore_async_mirror')",
    )
    .unwrap_or_else(|error| pgrx::error!("inspect async mirror publication: {error}"))
    .unwrap_or(false);
    if !publication_exists {
        pgrx::Spi::run("CREATE PUBLICATION koldstore_async_mirror")
            .unwrap_or_else(|error| pgrx::error!("create async mirror publication: {error}"));
    }
    unsafe {
        pgrx::pg_sys::PopActiveSnapshot();
        pgrx::pg_sys::CommitTransactionCommand();
    }
}

fn create_logical_slot(slot: &str) {
    let slot = CString::new(slot).expect("deterministic slot name contains no NUL");
    let plugin = c"pgoutput";
    let mut reader = pgrx::pg_sys::XLogReaderRoutine {
        page_read: Some(read_local_xlog_page),
        segment_open: Some(wal_segment_open),
        segment_close: Some(wal_segment_close),
    };

    unsafe {
        pgrx::pg_sys::CheckSlotPermissions();
        pgrx::pg_sys::CheckLogicalDecodingRequirements();
        // Ephemeral until initialization completes, so PostgreSQL removes a
        // partial slot automatically if the worker exits with an error.
        #[cfg(any(feature = "pg15", feature = "pg16"))]
        pgrx::pg_sys::ReplicationSlotCreate(
            slot.as_ptr(),
            true,
            pgrx::pg_sys::ReplicationSlotPersistency::RS_EPHEMERAL,
            false,
        );
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        pgrx::pg_sys::ReplicationSlotCreate(
            slot.as_ptr(),
            true,
            pgrx::pg_sys::ReplicationSlotPersistency::RS_EPHEMERAL,
            false,
            false,
            false,
        );
        let context = pgrx::pg_sys::CreateInitDecodingContext(
            plugin.as_ptr(),
            ptr::null_mut(),
            false,
            pgrx::pg_sys::InvalidXLogRecPtr.into(),
            &mut reader,
            None,
            None,
            None,
        );
        pgrx::pg_sys::DecodingContextFindStartpoint(context);
        pgrx::pg_sys::FreeDecodingContext(context);
        pgrx::pg_sys::ReplicationSlotPersist();
        pgrx::pg_sys::ReplicationSlotRelease();
    }
}

unsafe extern "C-unwind" fn read_local_xlog_page(
    state: *mut pgrx::pg_sys::XLogReaderState,
    target_page: pgrx::pg_sys::XLogRecPtr,
    requested_length: std::os::raw::c_int,
    target_record: pgrx::pg_sys::XLogRecPtr,
    buffer: *mut std::os::raw::c_char,
) -> std::os::raw::c_int {
    unsafe {
        pgrx::pg_sys::read_local_xlog_page(
            state,
            target_page,
            requested_length,
            target_record,
            buffer,
        )
    }
}

unsafe extern "C-unwind" fn wal_segment_open(
    state: *mut pgrx::pg_sys::XLogReaderState,
    segment: pgrx::pg_sys::XLogSegNo,
    timeline: *mut pgrx::pg_sys::TimeLineID,
) {
    unsafe { pgrx::pg_sys::wal_segment_open(state, segment, timeline) }
}

unsafe extern "C-unwind" fn wal_segment_close(state: *mut pgrx::pg_sys::XLogReaderState) {
    unsafe { pgrx::pg_sys::wal_segment_close(state) }
}

/// Persistent worker entry point for bounded-lag committed-WAL application.
#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_async_mirror_applier_main(argument: pgrx::pg_sys::Datum) {
    let database_oid = argument.value() as u32;
    attach_applier_signal_handlers();
    BackgroundWorker::connect_worker_to_spi_by_oid(
        Some(pgrx::pg_sys::Oid::from(database_oid)),
        None,
    );

    let slot = slot_name(database_oid);
    let slot_c = CString::new(slot.as_str()).expect("deterministic slot name contains no NUL");
    while !super::lifecycle::native_slot_exists_cstr(&slot_c) {
        if !BackgroundWorker::wait_latch(Some(APPLY_INTERVAL)) {
            return;
        }
    }
    let mut last_checked_wal = None;
    loop {
        let keep_running = super::lifecycle::native_slot_exists_cstr(&slot_c);
        if keep_running {
            let current_wal = current_wal_position();
            if last_checked_wal != Some(current_wal) {
                worker_transaction(|| {
                    // SPI may assign an XID even when pgoutput yields no rows.
                    // Capture the WAL position after commit below so that WAL
                    // generated by this attempt does not wake it again.
                    super::apply::apply_available().unwrap_or_else(|error| {
                        pgrx::error!("async mirror background apply failed: {error}")
                    });
                });
                last_checked_wal = Some(current_wal_position());
            }
        }
        if !keep_running || !BackgroundWorker::wait_latch(Some(APPLY_INTERVAL)) {
            break;
        }
        if BackgroundWorker::sighup_received() {
            unsafe { pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP) };
        }
    }
}

fn attach_applier_signal_handlers() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP);
    // Use PostgreSQL's standard SIGTERM handler while logical decoding is in
    // C code. It marks interrupts pending, allowing decoding and SPI safe
    // points to abort the transaction promptly during shutdown. pgrx's
    // cooperative SIGTERM flag is observed only after control returns to Rust.
    unsafe {
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        pgrx::pg_sys::pqsignal(pgrx::pg_sys::SIGTERM as i32, Some(applier_sigterm));
        #[cfg(feature = "pg18")]
        pgrx::pg_sys::pqsignal_be(pgrx::pg_sys::SIGTERM as i32, Some(applier_sigterm));
    }
}

unsafe extern "C-unwind" fn applier_sigterm(signal: std::os::raw::c_int) {
    unsafe { pgrx::pg_sys::die(signal) }
}

fn worker_transaction<R>(body: impl FnOnce() -> R) -> R {
    // pgrx's guarded worker transaction assigns an XID even to a read-only
    // poll. That creates a commit WAL record, which would make the applier
    // continuously wake itself. PostgreSQL cleans up this worker transaction
    // if an ERROR unwinds through the guarded worker entry point.
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

fn current_wal_position() -> u64 {
    unsafe { pgrx::pg_sys::GetXLogInsertRecPtr() }
}
