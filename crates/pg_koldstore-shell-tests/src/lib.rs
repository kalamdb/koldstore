//! PostgreSQL-free shell and contract tests for `pg_koldstore`.
//!
//! These tests previously lived under `crates/pg_koldstore/tests/` and blocked
//! `cargo pgrx test` by linking as native pg-feature binaries. Keeping them in a
//! sibling crate that always depends on `pg_koldstore` with `default-features =
//! false` restores a clean pgrx in-server test path.

#![deny(clippy::unwrap_used)]
