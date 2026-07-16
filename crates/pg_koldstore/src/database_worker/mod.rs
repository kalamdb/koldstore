//! PostgreSQL adapter for [`koldstore_worker`] database-scoped background work.
//!
//! Owns `pgrx` background-worker registration, latch polling, and SPI connection.
//! Ensure decisions and naming come from the PostgreSQL-free library crate.

#[cfg(feature = "pg")]
mod ensure;
#[cfg(feature = "pg")]
mod launcher;
#[cfg(feature = "pg")]
mod r#loop;

#[cfg(feature = "pg")]
pub use ensure::ensure_async_mirror_worker_pg;
#[cfg(feature = "pg")]
pub(crate) use ensure::{
    ensure_async_mirror_worker_once_if_needed, mark_worker_not_ensured, require_async_mirror_worker,
};
#[cfg(feature = "pg")]
pub(crate) use launcher::register_if_shared_preload as register_launcher_if_shared_preload;
#[cfg(feature = "pg")]
pub(crate) use r#loop::run_async_mirror_applier;
