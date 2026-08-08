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

mod dispatch;
mod ensure_pause;
mod identity;
mod policy;
mod supervisor;
mod wake;

/// Flush workflow and its lower storage stack.
pub use koldstore_flush as flush;
/// WAL-applier lifecycle and mirror stack.
pub use koldstore_wal_mirror as wal;

pub use dispatch::{
    deadline_delay, min_optional_duration, next_wait_duration, DynamicWorkerKind,
    RegistrationBackoff, REGISTRATION_RETRY_MAX, REGISTRATION_RETRY_MIN, SAFETY_RECONCILE_INTERVAL,
};
pub use ensure_pause::EnsurePauseSet;
pub use identity::{
    database_oid_from_worker_backend_type, flush_executor_worker_type, maintenance_worker_type,
    DatabaseOid,
};
pub use policy::LIBRARY_NAME;
pub use supervisor::{
    DatabaseWorkSnapshot, SupervisorPid, SupervisorRegistry, EVENT_FLUSH_QUEUE_DIRTY,
    EVENT_RECOVERY_REQUIRED, EVENT_SCHEDULE_DIRTY, EVENT_WAL_DIRTY, SUPERVISOR_REGISTRY_CAPACITY,
};
pub use wake::TransactionDirty;
