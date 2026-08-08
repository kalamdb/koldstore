//! PostgreSQL-free primitives for the KoldStore supervision tree.
//!
//! `koldstore-supervisor` is the top orchestration crate. It exposes the two
//! independently scheduled services beneath it:
//!
//! - [`flush`] — durable hot-to-cold work, backed by manifest/storage/Parquet.
//! - [`wal`] — near-realtime WAL application, backed by the mirror contract.
//!
//! The crate also owns worker identity, transaction-local dirty tracking,
//! diagnostic dispatch pausing, and lock-free cluster-supervisor state. Actual
//! PostgreSQL process lifecycle, SPI, latches, and worker entry points remain in
//! `pg_koldstore`.

mod ensure_pause;
mod identity;
mod policy;
mod supervisor;
mod wake;

/// Flush workflow and its lower storage stack.
pub use koldstore_flush as flush;
/// WAL-applier lifecycle and mirror stack.
pub use koldstore_wal as wal;

pub use ensure_pause::EnsurePauseSet;
pub use identity::{flush_executor_worker_type, maintenance_worker_type, DatabaseOid};
pub use policy::LIBRARY_NAME;
pub use supervisor::{
    DatabaseWorkSnapshot, SupervisorPid, SupervisorRegistry, EVENT_FLUSH_QUEUE_DIRTY,
    EVENT_RECOVERY_REQUIRED, EVENT_SCHEDULE_DIRTY, EVENT_WAL_DIRTY, SUPERVISOR_REGISTRY_CAPACITY,
};
pub use wake::TransactionDirty;
