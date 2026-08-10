//! In-server PostgreSQL tests for `pg_koldstore` using native `#[pg_test]`.
//!
//! These run inside a temporary cluster via `cargo pgrx test`. Keep multi-process,
//! object-store, and crash/restart scenarios in `tests/e2e`.
//!
//! Cold-only multi-type query/join smoke coverage lives in `cold_queries.inc.rs`
//! (filter with `cold_query`). Cold WHERE clauses must use indexed columns
//! (created before `manage_table`); joins that need cold non-PK quals use
//! `WITH … AS MATERIALIZED` so join predicates run on fetched rows.
//!
//! Async capture needs a logical slot. `register_temp_storage` pre-provisions it
//! before any SPI write so `#[pg_test]`'s wrapping transaction does not deadlock
//! slot creation.
//!
//! Flush fixtures must seed heap rows **before** `manage_*` so activation backfill
//! fills `__cl`. Post-manage DML is not WAL-visible until commit, which `#[pg_test]`
//! never does mid-body.
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
        assert_finishes_under, change_log_mirror_relation, create_messages_table, flush_table_rows,
        hold_apply_lock_for_populated_manage, jsonb_obj, manage_for_cold_flush, manage_shared,
        preprovision_async_mirror, register_temp_storage, setup_cold_typed_join_fixture,
        spi_get_explain, spi_get_i64, spi_get_text, spi_succeeds, unique_suffix, COLD_FACT_IDS,
        COLD_QUERY_BUDGET,
    };

    include!("lifecycle.inc.rs");
    include!("manage.inc.rs");
    include!("mirror_dml.inc.rs");
    include!("async_mirror_worker.inc.rs");
    include!("flush_scheduler.inc.rs");
    include!("session.inc.rs");
    include!("scan.inc.rs");
    include!("cold_queries.inc.rs");
}
