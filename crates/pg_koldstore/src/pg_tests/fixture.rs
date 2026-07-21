//! Shared helpers for in-server `#[pg_test]` fixtures.

use pgrx::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Default per-query budget for cold-path smoke tests (small fixtures).
pub(crate) const COLD_QUERY_BUDGET: Duration = Duration::from_secs(5);

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

/// Manages a table so force-flush prunes nearly all live rows into cold storage.
///
/// `hot_row_limit => 1` keeps at most one hot row after flush (newest by `id`).
pub(crate) fn manage_for_cold_flush(relation: &str, storage: &str) {
    let sql = format!(
        r#"
        SELECT koldstore.manage_table(
          table_name     => '{relation}'::regclass,
          storage        => '{storage}',
          hot_row_limit  => 1,
          min_flush_rows => 1,
          max_rows_per_file => 1000,
          migration_order_by => 'id'
        )
        "#
    );
    Spi::run(&sql).expect("manage_table for cold flush");
}

/// Force-flushes and asserts the job completed with at least `min_rows` flushed.
pub(crate) fn flush_table_rows_completed(relation: &str, min_rows: i64) -> i64 {
    let job_id = spi_get_text(&format!(
        "SELECT koldstore.flush_table('{relation}'::regclass, force => true)::text"
    ));
    let status = spi_get_text(&format!(
        "SELECT status::text FROM koldstore.jobs WHERE id = '{job_id}'::uuid"
    ));
    let rows = spi_get_i64(&format!(
        "SELECT COALESCE(rows_flushed, 0) FROM koldstore.jobs WHERE id = '{job_id}'::uuid"
    ));
    if status != "completed" || rows < min_rows {
        let err = Spi::get_one::<String>(&format!(
            "SELECT COALESCE(error_trace, '') FROM koldstore.jobs WHERE id = '{job_id}'::uuid"
        ))
        .ok()
        .flatten()
        .unwrap_or_default();
        panic!(
            "flush did not complete as expected for {relation}: status={status} rows_flushed={rows} min_rows={min_rows} err={err}"
        );
    }
    rows
}

/// Returns a single text column from a one-row query.
pub(crate) fn spi_get_text(sql: &str) -> String {
    Spi::get_one::<String>(sql)
        .expect("spi get_one")
        .expect("expected non-null text")
}

/// Returns the full multiline `EXPLAIN` / `EXPLAIN ANALYZE` text.
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

/// Runs `body` and asserts it finishes within `budget` (crash/stuck/slow guard).
pub(crate) fn assert_finishes_under(budget: Duration, body: impl FnOnce()) {
    let started = Instant::now();
    body();
    let elapsed = started.elapsed();
    assert!(
        elapsed <= budget,
        "query exceeded budget: elapsed={elapsed:?} budget={budget:?}"
    );
}

/// Cold-only multi-type facts table plus a plain dimension for join coverage.
pub(crate) struct ColdTypedJoinFixture {
    pub facts: String,
    pub accounts: String,
    pub row_count: i64,
}

/// Builds a multi-datatype managed table with secondary indexes, flushes to cold,
/// and a plain dim table for joins.
///
/// Indexes are created **before** `manage_table` so cold filter pushdown can use them.
/// After flush (`hot_row_limit=1`), prefer `id IN (1..7)` for cold-only predicates.
pub(crate) fn setup_cold_typed_join_fixture(label: &str) -> ColdTypedJoinFixture {
    let suffix = unique_suffix(label);
    let schema = format!("pgtest_{suffix}");
    let facts = format!("{schema}.facts");
    let accounts = format!("{schema}.accounts");
    let storage = register_temp_storage(&suffix);

    Spi::run(&format!("CREATE SCHEMA {schema}")).expect("create schema");
    Spi::run(&format!(
        r#"
        CREATE TABLE {facts} (
          id bigint PRIMARY KEY,
          account_id bigint NOT NULL,
          flag boolean,
          qty integer,
          amount double precision,
          note text,
          tag uuid,
          payload jsonb,
          ts timestamptz,
          category text
        );
        CREATE INDEX facts_account_id_idx ON {facts} (account_id);
        CREATE INDEX facts_category_idx ON {facts} (category);
        CREATE INDEX facts_note_idx ON {facts} (note);
        CREATE INDEX facts_ts_idx ON {facts} (ts);
        CREATE INDEX facts_tag_idx ON {facts} (tag);
        "#
    ))
    .expect("create facts + indexes");
    Spi::run(&format!(
        r#"
        CREATE TABLE {accounts} (
          account_id bigint PRIMARY KEY,
          account_name text NOT NULL
        )
        "#
    ))
    .expect("create accounts");

    Spi::run(&format!(
        r#"
        INSERT INTO {facts} (
          id, account_id, flag, qty, amount, note, tag, payload, ts, category
        ) VALUES
          (1, 1, true,  10, 10.5, 'alpha',
             '11111111-1111-1111-1111-111111111111'::uuid,
             '{{"k":"v1","n":1,"labels":["a","x"]}}'::jsonb,
             '2024-01-01 10:00:00+00'::timestamptz, 'odd'),
          (2, 1, false, 20, 20.0, 'bravo',
             '22222222-2222-2222-2222-222222222222'::uuid,
             '{{"k":"v2","n":2,"labels":["b"]}}'::jsonb,
             '2024-01-02 10:00:00+00'::timestamptz, 'even'),
          (3, 2, true,  30, 30.25, 'charlie',
             '33333333-3333-3333-3333-333333333333'::uuid,
             '{{"k":"v3","n":3,"labels":["a","c"]}}'::jsonb,
             '2024-01-03 10:00:00+00'::timestamptz, 'odd'),
          (4, 2, false, 40, 40.0, 'delta',
             '44444444-4444-4444-4444-444444444444'::uuid,
             '{{"k":"v4","n":4,"labels":["d"]}}'::jsonb,
             '2024-01-04 10:00:00+00'::timestamptz, 'even'),
          (5, 3, true,  50, 50.75, 'echo',
             '55555555-5555-5555-5555-555555555555'::uuid,
             '{{"k":"v5","n":5,"labels":["a","e"]}}'::jsonb,
             '2024-01-05 10:00:00+00'::timestamptz, 'odd'),
          (6, 3, false, 60, 60.0, 'foxtrot',
             '66666666-6666-6666-6666-666666666666'::uuid,
             '{{"k":"v6","n":6,"labels":["f"]}}'::jsonb,
             '2024-01-06 10:00:00+00'::timestamptz, 'even'),
          (7, 3, NULL,  70, NULL, NULL,
             NULL, NULL, NULL, 'odd'),
          (8, 99, true, 80, 80.0, 'orphan-fact',
             '88888888-8888-8888-8888-888888888888'::uuid,
             '{{"k":"v8","n":8,"labels":["z"]}}'::jsonb,
             '2024-01-08 10:00:00+00'::timestamptz, 'even')
        "#
    ))
    .expect("seed facts");

    Spi::run(&format!(
        r#"
        INSERT INTO {accounts} (account_id, account_name) VALUES
          (1, 'acct-one'),
          (2, 'acct-two'),
          (3, 'acct-three'),
          (50, 'acct-orphan-dim')
        "#
    ))
    .expect("seed accounts");

    manage_for_cold_flush(&facts, &storage);
    let flushed = flush_table_rows_completed(&facts, 7);
    assert!(
        flushed >= 7,
        "expected at least 7 cold rows flushed, got rows_flushed={flushed}"
    );
    let cold = spi_get_i64(&format!(
        "SELECT (koldstore.describe_table('{facts}'::regclass)->>'cold_row_count')::bigint"
    ));
    let hot = spi_get_i64(&format!(
        "SELECT (koldstore.describe_table('{facts}'::regclass)->>'hot_rows')::bigint"
    ));
    assert!(
        cold >= 7,
        "expected >=7 cold rows after flush, got hot={hot} cold={cold}"
    );
    assert!(
        hot <= 1,
        "expected at most 1 hot row after flush, got hot={hot} cold={cold}"
    );

    ColdTypedJoinFixture {
        facts,
        accounts,
        row_count: 8,
    }
}

/// Cold fact ids after flush with `hot_row_limit=1` (newest id=8 stays hot).
pub(crate) const COLD_FACT_IDS: &str = "1,2,3,4,5,6,7";

/// JSONB accessor that tolerates cold string-scalar encoding.
pub(crate) fn jsonb_obj(expr: &str) -> String {
    format!(
        "(CASE WHEN jsonb_typeof({expr}) = 'string' THEN ({expr} #>> '{{}}')::jsonb ELSE {expr} END)"
    )
}
