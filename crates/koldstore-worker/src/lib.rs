//! Database-scoped background worker orchestration.
//!
//! Owns ensure-decision logic, worker identity naming, poll policy, the task
//! trait used by long-lived database workers, and flush-check cadence helpers.
//! Must not depend on `pgrx`, SPI, or PostgreSQL symbols — the extension adapter
//! in `pg_koldstore` wires those.

mod ensure;
mod identity;
mod policy;
mod scheduler;
mod task;

pub use ensure::{ensure_action, EnsureAction};
pub use identity::{async_mirror_worker_type, DatabaseOid};
pub use policy::{APPLY_POLL_INTERVAL_MS, LAUNCHER_POLL_INTERVAL_MS, LIBRARY_NAME};
pub use scheduler::flush_check_due;
pub use task::{DatabaseWorkerTask, TickResult};
