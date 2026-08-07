//! PostgreSQL-free shell and contract tests for `pg_koldstore`.
//!
//! These tests previously lived under `crates/pg_koldstore/tests/` and blocked
//! `cargo pgrx test` by linking as native pg-feature binaries. Keeping them in a
//! sibling crate that always depends on `pg_koldstore` with `default-features =
//! false` restores a clean pgrx in-server test path.
//!
//! Do **not** move these back under `crates/pg_koldstore/tests/` — that re-breaks
//! the pgrx harness. Keep packaging-only filesystem checks (e.g.
//! `extension_upgrade.rs`) in `pg_koldstore/tests/`; keep live CREATE/DROP
//! EXTENSION coverage under `tests/e2e/suite/extension_lifecycle.rs`.

#![deny(clippy::unwrap_used)]
