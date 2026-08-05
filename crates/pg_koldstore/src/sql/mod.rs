//! PostgreSQL SQL entrypoints.
//!
//! Library crates own SQL planning; these modules execute plans through SPI.
//! Layout follows extension domains: migrate, flush, events, storage, plus
//! small shared helpers (`job_lock`, `session`, `sort_key`, `ops`).

#[cfg(feature = "pg")]
pub mod events;
pub mod flush;
#[cfg(feature = "pg")]
pub mod job_lock;
pub mod migrate;
pub mod ops;
pub mod session;
pub mod sort_key;
pub mod storage;
