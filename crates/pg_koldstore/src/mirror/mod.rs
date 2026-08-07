//! WAL-backed asynchronous mirror capture and apply.
//!
//! Ownership:
//! - `lifecycle` — slot / publication / advisory locks
//! - `apply` — fixed-fence SPI peek/apply/advance (idempotent latest-state upserts)
//! - `provision` — one-shot slot provisioner worker
//! - `worker` — C entry point for the ephemeral database-maintenance worker
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
pub mod worker;
