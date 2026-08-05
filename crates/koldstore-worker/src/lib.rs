//! Database-scoped background worker orchestration.
//!
//! Owns ensure-decision logic, worker identity naming, wake policy, tick
//! outcomes used by long-lived database workers, and flush-check cadence
//! helpers. Must not depend on `pgrx`, SPI, or PostgreSQL symbols — the
//! extension adapter in `pg_koldstore` wires those.

mod ensure;
mod identity;
mod policy;
mod scheduler;
mod task;
mod wake;

pub use ensure::{ensure_action, EnsureAction};
pub use identity::{async_mirror_worker_type, flush_executor_worker_type, DatabaseOid};
pub use policy::{
    next_soft_fail_backoff_ms, LAUNCHER_POLL_INTERVAL_MS, LIBRARY_NAME,
    MAX_IMMEDIATE_PENDING_TICKS, WAKE_REGISTRY_CAPACITY,
};
pub use scheduler::{flush_check_due, millis_until_flush_check, PendingDrainBudget};
pub use task::TickResult;
pub use wake::{
    AtomicWakeRegistry, EmptyWakeRetry, PublishWake, TransactionDirty, WakeCursor, WakeGeneration,
    WorkerPid,
};
