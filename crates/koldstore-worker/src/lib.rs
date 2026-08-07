//! Database-scoped background worker orchestration.
//!
//! Owns PostgreSQL-free worker identity, wake policy, durable-event generations,
//! and shared supervisor state. PostgreSQL integration lives in `pg_koldstore`.

mod ensure;
mod ensure_pause;
mod identity;
mod policy;
mod scheduler;
mod supervisor;
mod task;
mod wake;

pub use ensure::{ensure_action, EnsureAction};
pub use ensure_pause::EnsurePauseSet;
pub use identity::{async_mirror_worker_type, flush_executor_worker_type, DatabaseOid};
pub use policy::{
    next_soft_fail_backoff_ms, LAUNCHER_POLL_INTERVAL_MS, LIBRARY_NAME,
    MAX_IMMEDIATE_PENDING_TICKS, WAKE_REGISTRY_CAPACITY,
};
pub use scheduler::{flush_check_due, millis_until_flush_check, PendingDrainBudget};
pub use supervisor::{
    DatabaseWorkSnapshot, SupervisorPid, SupervisorRegistry, EVENT_FLUSH_QUEUE_DIRTY,
    EVENT_RECOVERY_REQUIRED, EVENT_SCHEDULE_DIRTY, EVENT_WAL_DIRTY,
    SUPERVISOR_REGISTRY_CAPACITY,
};
pub use task::TickResult;
pub use wake::{
    AtomicWakeRegistry, EmptyWakeRetry, PublishWake, TransactionDirty, WakeCursor, WakeGeneration,
    WorkerPid,
};
