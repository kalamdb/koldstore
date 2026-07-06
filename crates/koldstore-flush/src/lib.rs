//! Hot-to-cold flush workflow planning.
//!
//! Owns flush eligibility, job state transitions, manifest sync planning, segment
//! cleanup, and recovery classification. Must not depend on `pgrx`. PostgreSQL job
//! enqueue and SPI execution stay in `pg_koldstore`.

pub mod cleanup;
pub mod job;
pub mod ops;
pub mod policy;
pub mod recovery;
pub mod worker;

pub use koldstore_jobs::{JobId, JobStatus, JobType, LeaseEpoch, StaleLeaseAction};
pub use ops::*;
