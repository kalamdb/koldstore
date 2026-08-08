//! WAL-backed asynchronous mirror capture and apply.
//!
//! Ownership:
//! - `lifecycle` — slot / publication / advisory locks
//! - `apply` — fixed-fence SPI peek/apply/advance (idempotent latest-state upserts)
//! - `provision` — one-shot slot provisioner worker
//!
//! Ephemeral maintenance-worker registration and its C entry point live under
//! `crate::worker`; the PostgreSQL-free decoder lives in `koldstore_wal_mirror`.

#[cfg(feature = "pg")]
pub mod apply;
#[cfg(feature = "pg")]
pub mod lifecycle;
#[cfg(feature = "pg")]
pub mod provision;
#[cfg(feature = "pg")]
pub mod status;
