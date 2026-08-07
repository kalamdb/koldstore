//! PostgreSQL-free primitives for KoldStore background work.
//!
//! The crate intentionally contains only the small pieces shared by the
//! PostgreSQL adapter: worker identity, transaction-local dirty tracking,
//! test/diagnostic dispatch pausing, and lock-free supervisor state. Process
//! lifecycle and SQL scheduling live in `pg_koldstore`.

mod ensure_pause;
mod identity;
mod policy;
mod supervisor;
mod wake;

pub use ensure_pause::EnsurePauseSet;
pub use identity::{async_mirror_worker_type, flush_executor_worker_type, DatabaseOid};
pub use policy::LIBRARY_NAME;
pub use supervisor::{
    DatabaseWorkSnapshot, SupervisorPid, SupervisorRegistry, EVENT_FLUSH_QUEUE_DIRTY,
    EVENT_RECOVERY_REQUIRED, EVENT_SCHEDULE_DIRTY, EVENT_WAL_DIRTY, SUPERVISOR_REGISTRY_CAPACITY,
};
pub use wake::TransactionDirty;
