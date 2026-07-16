//! Database-scoped background worker orchestration for KoldStore.
//!
//! Owns ensure-decision logic, worker identity naming, poll policy, and the
//! task trait used by long-lived database workers. Must not depend on `pgrx`,
//! SPI, or PostgreSQL symbols — the extension adapter in `pg_koldstore` wires
//! those. Async mirror apply is the first task; flush job running is a planned
//! future implementor of [`DatabaseWorkerTask`].

mod ensure;
mod identity;
mod policy;
mod task;

pub use ensure::{ensure_action, EnsureAction};
pub use identity::{async_mirror_worker_type, DatabaseOid};
pub use policy::{APPLY_POLL_INTERVAL_MS, LAUNCHER_POLL_INTERVAL_MS, LIBRARY_NAME};
pub use task::{DatabaseWorkerTask, TickResult};
