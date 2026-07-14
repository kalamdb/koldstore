//! Shared helpers for in-server `#[pg_test]` fixtures.

use pgrx::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Returns a unique SQL identifier suffix for this test run.
pub(crate) fn unique_suffix(label: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!("{label}_{id}")
}

/// Registers filesystem storage under a unique temp directory and returns its name.
pub(crate) fn register_temp_storage(label: &str) -> String {
    let name = unique_suffix(label);
    let root: PathBuf = std::env::temp_dir().join(format!("koldstore-pg-test-{name}"));
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

/// Manages a shared table with flush-friendly settings for small fixtures.
pub(crate) fn manage_shared(relation: &str, storage: &str) {
    let sql = format!(
        r#"
        SELECT koldstore.manage_table(
          table_name     => '{relation}'::regclass,
          storage        => '{storage}',
          hot_row_limit  => 1000,
          min_flush_rows => 1,
          max_rows_per_file => 1000
        )
        "#
    );
    Spi::run(&sql).expect("manage_table");
}

/// Returns a single text column from a one-row query.
pub(crate) fn spi_get_text(sql: &str) -> String {
    Spi::get_one::<String>(sql)
        .expect("spi get_one")
        .expect("expected non-null text")
}

/// Returns a single i64 column from a one-row query.
pub(crate) fn spi_get_i64(sql: &str) -> i64 {
    Spi::get_one::<i64>(sql)
        .expect("spi get_one i64")
        .expect("expected non-null i64")
}

/// Returns whether a SQL statement succeeds.
pub(crate) fn spi_succeeds(sql: &str) -> bool {
    Spi::run(sql).is_ok()
}

/// Runs `flush_table` and returns `rows_flushed` from the resulting job.
pub(crate) fn flush_table_rows(relation: &str, force: bool) -> i64 {
    let force_sql = if force { "true" } else { "false" };
    let job_id = spi_get_text(&format!(
        "SELECT koldstore.flush_table('{relation}'::regclass, force => {force_sql})::text"
    ));
    spi_get_i64(&format!(
        "SELECT rows_flushed FROM koldstore.jobs WHERE id = '{job_id}'::uuid"
    ))
}
