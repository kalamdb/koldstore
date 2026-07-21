//! Shared helpers for in-server `#[pg_bench]` fixtures.
//!
//! Goal: measure extension overhead against plain Postgres paths in-process.
//! Client-scale / multi-side comparisons stay in `tests/storage` and `benchmarks/`.

use pgrx::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Returns a unique SQL identifier suffix for this bench run.
pub(crate) fn unique_suffix(label: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{label}_{id}")
}

/// Ensures the session-local bench context table exists and is empty.
pub(crate) fn reset_bench_ctx() {
    Spi::run(
        "CREATE TEMP TABLE IF NOT EXISTS pg_bench_ctx (
            key text PRIMARY KEY,
            value text NOT NULL
        )",
    )
    .expect("create pg_bench_ctx");
    Spi::run("TRUNCATE pg_bench_ctx").expect("truncate pg_bench_ctx");
}

/// Stores a string value under `key` in the session-local bench context.
pub(crate) fn stash(key: &str, value: &str) {
    Spi::run(&format!(
        "INSERT INTO pg_bench_ctx (key, value) VALUES ('{key}', '{value}')
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
    ))
    .expect("stash bench ctx");
}

/// Reads a previously stashed context value.
pub(crate) fn ctx(key: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT value FROM pg_bench_ctx WHERE key = '{key}'"
    ))
    .expect("spi get ctx")
    .unwrap_or_else(|| panic!("missing bench ctx key `{key}`"))
}

/// Registers filesystem storage under a unique temp directory and returns its name.
pub(crate) fn register_temp_storage(label: &str) -> String {
    let name = unique_suffix(label);
    let root: PathBuf = std::env::temp_dir().join(format!("koldstore-pg-bench-{name}"));
    if root.exists() {
        let _ = std::fs::remove_dir_all(&root);
    }
    std::fs::create_dir_all(&root).expect("create temp storage root");
    let root_str = root.to_str().expect("utf-8 storage path");
    let sql = format!(
        "SELECT koldstore.register_storage('{name}', 'filesystem', '{root_str}', '{{}}'::jsonb, '{{}}'::jsonb)"
    );
    Spi::run(&sql).expect("register_storage");
    name
}

/// Creates a simple heap table with a bigint primary key and text body.
pub(crate) fn create_messages_table(schema: &str, table: &str) {
    Spi::run(&format!("CREATE SCHEMA IF NOT EXISTS {schema}")).expect("create schema");
    Spi::run(&format!(
        "CREATE TABLE {schema}.{table} (id bigint PRIMARY KEY, body text NOT NULL)"
    ))
    .expect("create messages table");
}

/// Manages a shared table with a high flush threshold so hot-path benches stay hot.
pub(crate) fn manage_shared(relation: &str, storage: &str) {
    let sql = format!(
        r#"
        SELECT koldstore.manage_table(
          table_name     => '{relation}'::regclass,
          storage        => '{storage}',
          hot_row_limit  => 10000,
          min_flush_rows => 100000,
          max_rows_per_file => 1000
        )
        "#
    );
    Spi::run(&sql).expect("manage_table");
}

/// Manages a table with a low flush threshold so force-flush benches move rows quickly.
pub(crate) fn manage_flushable(relation: &str, storage: &str) {
    let sql = format!(
        r#"
        SELECT koldstore.manage_table(
          table_name     => '{relation}'::regclass,
          storage        => '{storage}',
          hot_row_limit  => 100,
          min_flush_rows => 1,
          max_rows_per_file => 1000
        )
        "#
    );
    Spi::run(&sql).expect("manage_table flushable");
}

/// Returns a single i64 column from a one-row query.
pub(crate) fn spi_get_i64(sql: &str) -> i64 {
    Spi::get_one::<i64>(sql)
        .expect("spi get_one i64")
        .expect("expected non-null i64")
}

/// Returns a single text column from a one-row query.
pub(crate) fn spi_get_text(sql: &str) -> String {
    Spi::get_one::<String>(sql)
        .expect("spi get_one text")
        .expect("expected non-null text")
}

/// Seeds `n` rows with ids `1..=n` (idempotent via ON CONFLICT DO NOTHING).
pub(crate) fn seed_rows(relation: &str, n: i64) {
    Spi::run(&format!(
        "INSERT INTO {relation} (id, body)
         SELECT g, 'seed-' || g::text FROM generate_series(1, {n}) AS g
         ON CONFLICT DO NOTHING"
    ))
    .expect("seed rows");
}

/// Creates an unmanaged messages table and stashes `relation`.
pub(crate) fn prepare_plain_messages(label: &str) -> String {
    reset_bench_ctx();
    let suffix = unique_suffix(label);
    let schema = format!("pgbench_{suffix}");
    let relation = format!("{schema}.messages");
    create_messages_table(&schema, "messages");
    stash("relation", &relation);
    relation
}

/// Creates and manages a messages table; stashes `relation` (+ optional `storage`).
pub(crate) fn prepare_managed_messages(label: &str, flushable: bool) -> String {
    reset_bench_ctx();
    let suffix = unique_suffix(label);
    let schema = format!("pgbench_{suffix}");
    let relation = format!("{schema}.messages");
    let storage = register_temp_storage(&suffix);
    create_messages_table(&schema, "messages");
    if flushable {
        manage_flushable(&relation, &storage);
    } else {
        manage_shared(&relation, &storage);
    }
    stash("relation", &relation);
    stash("storage", &storage);
    relation
}

/// Runs `flush_table(..., force => true)` and returns `rows_flushed`.
pub(crate) fn flush_table_rows(relation: &str) -> i64 {
    let job_id = spi_get_text(&format!(
        "SELECT koldstore.flush_table('{relation}'::regclass, force => true)::text"
    ));
    spi_get_i64(&format!(
        "SELECT rows_flushed FROM koldstore.jobs WHERE id = '{job_id}'::uuid"
    ))
}

/// Collects EXPLAIN text for a query (multiline).
pub(crate) fn spi_get_explain(sql: &str) -> String {
    Spi::connect(|client| {
        let table = client.select(sql, None, &[]).expect("explain select");
        let mut lines = Vec::new();
        for row in table {
            let line: Option<String> = row.get(1).expect("explain column");
            if let Some(line) = line {
                lines.push(line);
            }
        }
        lines.join("\n")
    })
}
