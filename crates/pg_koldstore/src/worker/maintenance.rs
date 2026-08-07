//! Dynamic database-maintenance worker registration owned by the cluster supervisor.
//!
//! Maintenance workers are intentionally ephemeral. They connect to exactly one
//! database, drain committed WAL through a fixed durable fence, reconcile local
//! flush scheduling/recovery, then exit when the database is caught up.

use koldstore_worker::{async_mirror_worker_type, DatabaseOid, LIBRARY_NAME};
use pgrx::bgworkers::BackgroundWorkerBuilder;

const MAINTENANCE_FUNCTION: &str = "koldstore_async_mirror_applier_main";

/// Registers one already-reserved maintenance worker without waiting for startup.
///
/// Only the static cluster supervisor may call this in production. Shared state
/// reserves the database before registration so a second worker cannot race in
/// while PostgreSQL is still starting the first process.
pub(crate) fn register_maintenance_from_supervisor(database_oid: u32) -> Result<(), String> {
    let database_oid = DatabaseOid::new(database_oid);
    let worker_type = async_mirror_worker_type(database_oid);
    BackgroundWorkerBuilder::new(&worker_type)
        .set_type(&worker_type)
        .set_library(LIBRARY_NAME)
        .set_function(MAINTENANCE_FUNCTION)
        .enable_spi_access()
        .set_restart_time(None)
        .set_argument(Some(pgrx::pg_sys::Datum::from(database_oid.get())))
        .set_notify_pid(unsafe { pgrx::pg_sys::MyProcPid })
        .load_dynamic()
        .map(|_| ())
        .map_err(|_| {
            format!(
                "could not register database maintenance worker \
                 (worker_type={worker_type}; usually max_worker_processes exhausted)"
            )
        })
}
