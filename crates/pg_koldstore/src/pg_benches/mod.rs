//! In-server `#[pg_bench]` benchmarks for `pg_koldstore`.
//!
//! Run via `scripts/run-pgrx-bench.sh`. These measure extension overhead inside
//! one Postgres backend so regressions in hooks, SPI, manage/scan, and session
//! helpers show up before they affect a hosting cluster.
//!
//! Coverage (compare `plain_heap_*` vs `managed_hot_*` absolute times):
//! - plain heap baselines (insert / count / PK lookup / update)
//! - managed hot DML + scan (insert / count / PK / update / delete / ORDER BY LIMIT)
//! - session + catalog (version, snowflake, GUC, describe_table, EXPLAIN)
//! - lifecycle (`lifecycle_manage_table`, `lifecycle_unmanage_table`, `lifecycle_flush_table_force`)
//!
//! Not covered here (use other suites): multi-client load (`benchmarks/`),
//! storage/size/RSS (`tests/storage`), correctness (`tests/e2e`, `#[pg_test]`).
//!
//! `#[pgrx::pg_schema]` only accepts inline `mod { ... }` blocks, and `#[pg_bench]`
//! requires the schema module to be named `benches`, so bodies are `include!`d below.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[cfg(feature = "pg")]
mod fixture;

#[cfg(feature = "pg")]
#[pgrx::pg_schema]
mod benches {
    use pgrx::prelude::*;
    use pgrx_bench::{black_box, BatchSize, Bencher};

    use super::fixture::{
        create_messages_table, ctx, flush_table_rows, manage_shared, prepare_managed_messages,
        prepare_plain_messages, register_temp_storage, seed_rows, spi_get_explain, spi_get_i64,
        spi_get_text, unique_suffix,
    };

    include!("plain_heap.inc.rs");
    include!("managed_hot.inc.rs");
    include!("catalog_session.inc.rs");
    include!("lifecycle.inc.rs");
}
