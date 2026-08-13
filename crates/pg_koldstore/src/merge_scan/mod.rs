//! KoldMergeScan PostgreSQL glue.

pub use koldstore_merge::scan::{path, plan};

#[cfg(feature = "pg")]
pub mod pg;
pub mod reader_pool;
