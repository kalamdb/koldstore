//! KoldMergeScan PostgreSQL glue.

pub use koldstore_merge::scan::{exec, path, plan};

pub mod ffi;
#[cfg(feature = "pg")]
pub mod pg;
pub mod reader_pool;
