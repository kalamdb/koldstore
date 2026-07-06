//! Session SQL helpers.
//!
//! Re-exports pg-free session helpers from `koldstore-common` and exposes
//! PostgreSQL `#[pg_extern]` session functions here.

pub use koldstore_common::session::*;

use koldstore_common::snowflake;

/// Generates a monotonic Snowflake-like id for tests and SQL default use.
#[must_use]
#[cfg_attr(feature = "pg", pgrx::pg_extern(name = "snowflake_id"))]
pub fn snowflake_id() -> i64 {
    snowflake::next_id(snowflake_worker_id()).unwrap_or_else(raise_snowflake_error)
}

/// Returns the active user scope when available.
#[must_use]
#[cfg_attr(feature = "pg", pgrx::pg_extern(name = "koldstore_user_id"))]
pub fn koldstore_user_id() -> Option<String> {
    None
}

#[cfg(feature = "pg_test")]
const fn snowflake_worker_id() -> u16 {
    0
}

#[cfg(all(not(feature = "pg_test"), any(feature = "pg17", feature = "pg18")))]
fn snowflake_worker_id() -> u16 {
    let proc_number = unsafe { pgrx::pg_sys::MyProcNumber };
    normalize_postgres_worker_id(proc_number)
}

#[cfg(all(not(feature = "pg_test"), any(feature = "pg15", feature = "pg16")))]
fn snowflake_worker_id() -> u16 {
    let backend_id = unsafe { pgrx::pg_sys::MyBackendId };
    normalize_postgres_worker_id(backend_id)
}

#[cfg(not(any(feature = "pg", feature = "pg_test")))]
const fn snowflake_worker_id() -> u16 {
    0
}

#[cfg(all(not(feature = "pg_test"), feature = "pg"))]
fn normalize_postgres_worker_id(worker_id: i32) -> u16 {
    if worker_id <= 0 {
        return 0;
    }
    let worker_id = u16::try_from(worker_id)
        .unwrap_or_else(|_| pgrx::error!("PostgreSQL backend worker id {worker_id} is too large"));
    if worker_id > 1023 {
        pgrx::error!("PostgreSQL backend worker id {worker_id} exceeds Snowflake worker id limit");
    }
    worker_id
}

#[cfg(feature = "pg")]
fn raise_snowflake_error(error: snowflake::SnowflakeError) -> i64 {
    pgrx::error!("snowflake id generation failed: {error}")
}

#[cfg(not(feature = "pg"))]
fn raise_snowflake_error(error: snowflake::SnowflakeError) -> i64 {
    panic!("snowflake id generation failed: {error}")
}
