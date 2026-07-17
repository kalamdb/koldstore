//! Single E2E integration-test binary for local pgrx-backed PostgreSQL.
//!
//! One binary keeps nextest listing/startup cheap (especially on macOS) while
//! still grouping tests under category modules for `-E 'test(crash::)'` filters.

mod common;

mod crash;
mod dml;
mod equality;
mod flush;
mod isolation;
mod join;
mod merge;
mod migrate;
mod scope;
mod storage;
mod suite;
