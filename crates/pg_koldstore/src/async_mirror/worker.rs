//! C entry points for async-mirror background workers.

/// Persistent worker entry point for bounded-lag committed-WAL application.
#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn koldstore_async_mirror_applier_main(argument: pgrx::pg_sys::Datum) {
    let database_oid = argument.value() as u32;
    crate::database_worker::run_async_mirror_applier(database_oid);
}
