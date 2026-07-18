//! WAL-backed asynchronous mirror capture and apply.
//!
//! Ownership:
//! - `lifecycle` — slot / publication / advisory locks
//! - `apply` — SPI peek/apply/advance (idempotent latest-state upserts)
//! - `provision` — one-shot slot provisioner worker
//! - `task` — [`koldstore_worker::DatabaseWorkerTask`] for the shared DB worker
//! - `worker` — C entry point for the persistent applier
//!
//! The PostgreSQL-free `pgoutput` decoder lives in [`koldstore_mirror::pgoutput`].

#[cfg(feature = "pg")]
pub mod apply;
#[cfg(feature = "pg")]
pub mod lifecycle;
#[cfg(feature = "pg")]
pub mod provision;
#[cfg(feature = "pg")]
pub mod status;
#[cfg(feature = "pg")]
pub(crate) mod task;
#[cfg(feature = "pg")]
pub mod worker;

/// Re-export the library decoder for callers that historically imported
/// `koldstore::async_mirror::protocol`.
pub mod protocol {
    pub use koldstore_mirror::pgoutput::*;
}
