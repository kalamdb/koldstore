//! KoldMergeScan PostgreSQL glue.

pub use koldstore_merge::scan::{exec, path, plan};

#[cfg(feature = "pg")]
pub mod pg;
pub mod reader_pool;
