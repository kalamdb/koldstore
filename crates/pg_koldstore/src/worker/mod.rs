//! PostgreSQL adapter for [`koldstore_worker`] background work.
//!
//! The static cluster supervisor owns dynamic worker registration.  Legacy
//! per-database ensure exports remain temporarily while the WAL applier is moved
//! to the same event-driven supervisor lifecycle.

#[cfg(feature = "pg")]
mod ensure;
#[cfg(feature = "pg")]
mod flush_executor;
#[cfg(feature = "pg")]
mod flush_task;
#[cfg(feature = "pg")]
mod launcher;
#[cfg(feature = "pg")]
mod r#loop;
#[cfg(feature = "pg")]
pub(crate) mod txn;
#[cfg(feature = "pg")]
pub(crate) mod wake;

#[cfg(feature = "pg")]
pub use ensure::ensure_async_mirror_worker_pg;
#[cfg(feature = "pg")]
pub(crate) use ensure::{
    ensure_async_mirror_worker, ensure_async_mirror_worker_once_if_needed, mark_worker_not_ensured,
    require_async_mirror_worker,
};
#[cfg(feature = "pg")]
pub(crate) use flush_executor::{
    register_flush_executor_from_supervisor, spawn_flush_executor_if_needed,
    spawn_flush_executors_for_pending_work,
};
#[cfg(feature = "pg")]
pub use flush_task::run_flush_scheduler_tick_pg;
#[cfg(feature = "pg")]
pub(crate) use launcher::register_if_shared_preload as register_launcher_if_shared_preload;
#[cfg(feature = "pg")]
pub(crate) use r#loop::run_async_mirror_applier;
