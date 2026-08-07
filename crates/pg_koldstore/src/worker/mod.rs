//! PostgreSQL adapter for [`koldstore_worker`] background work.
//!
//! The static cluster supervisor owns dynamic worker registration. Database
//! maintenance workers and heavy flush executors are both ephemeral.

#[cfg(feature = "pg")]
mod control;
#[cfg(feature = "pg")]
mod flush_executor;
#[cfg(feature = "pg")]
mod flush_task;
#[cfg(feature = "pg")]
mod maintenance;
#[cfg(feature = "pg")]
mod supervisor;
#[cfg(feature = "pg")]
pub(crate) mod txn;
#[cfg(feature = "pg")]
pub(crate) mod wake;

#[cfg(feature = "pg")]
pub(crate) use control::require_async_mirror_worker;
#[cfg(feature = "pg")]
pub(crate) use flush_executor::{
    notify_flush_queue as spawn_flush_executor_if_needed, register_flush_executor_from_supervisor,
};
#[cfg(feature = "pg")]
pub use flush_task::run_flush_scheduler_tick_pg;
#[cfg(feature = "pg")]
pub(crate) use maintenance::register_maintenance_from_supervisor;
#[cfg(feature = "pg")]
pub(crate) use supervisor::register_if_shared_preload as register_supervisor_if_shared_preload;
