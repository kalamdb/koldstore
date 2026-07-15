//! In-server PostgreSQL tests for `pg_koldstore` using native `#[pg_test]`.
//!
//! These run inside a temporary cluster via `cargo pgrx test`. Keep multi-process,
//! object-store, and crash/restart scenarios in `tests/e2e`.
//!
//! `#[pgrx::pg_schema]` only accepts inline `mod { ... }` blocks, so test bodies are
//! `include!`d into the schema module below.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[cfg(feature = "pg")]
mod fixture;

#[cfg(feature = "pg")]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    use super::fixture::{
        create_messages_table, flush_table_rows, manage_shared, register_temp_storage, spi_get_i64,
        spi_get_explain, spi_get_text, spi_succeeds, unique_suffix,
    };

    include!("lifecycle.inc.rs");
    include!("manage.inc.rs");
    include!("mirror_dml.inc.rs");
    include!("session.inc.rs");
    include!("scan.inc.rs");
}
