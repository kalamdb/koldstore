//! Shared job framework for flush, migrate, and future background workflows.
//!
//! Owns lease records, phase/status enums, batch progression, and generic stale-lease
//! recovery. Must not depend on `pgrx` or workflow-specific flush/migrate logic.
//! New shared job mechanics belong here; domain phases stay in `koldstore-flush` or
//! `koldstore-migrate`.

pub mod lease;
pub mod model;
pub mod phase;

pub use lease::{JobLease, LeaseClaim, LeaseEpoch, LeaseSeconds, StaleLeaseAction};
pub use model::{JobId, JobStatus, JobType};
pub use phase::{JobPhase, PhaseTransition};
