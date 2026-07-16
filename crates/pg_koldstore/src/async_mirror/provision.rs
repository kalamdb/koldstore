//! One-shot background worker that creates the async mirror slot and publication.
//!
//! Logical slots cannot be created in the transaction that manages a table, so
//! this short-lived worker owns slot creation in an autonomous transaction.

use std::{ffi::CString, ptr};

use koldstore_worker::LIBRARY_NAME;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder};

use super::lifecycle::{slot_name, PUBLICATION_NAME};

const PROVISIONER_FUNCTION: &str = "koldstore_async_mirror_slot_provisioner_main";

/// Creates missing async infrastructure in an autonomous transaction.
///
/// The caller waits for worker shutdown and then validates the resulting slot,
/// so worker startup failures become a synchronous `manage_table` error.
///
/// # Errors
///
/// Returns an error when PostgreSQL cannot register, start, or stop the worker.
pub(crate) fn provision_infrastructure(database_oid: u32) -> Result<(), String> {
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
    let publication_exists = super::lifecycle::publication_exists()
        .unwrap_or_else(|error| pgrx::error!("inspect async mirror publication: {error}"));
    if !publication_exists {
        pgrx::Spi::run(&format!(
            "CREATE PUBLICATION {}",
            koldstore_common::quote_ident(PUBLICATION_NAME)
        ))
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
