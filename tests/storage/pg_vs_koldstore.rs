//! Compare plain PostgreSQL storage/speed with the same table under KoldStore.
//!
//! Schema: [`schema.sql`](schema.sql). Seeds a wide (~50 column) table, measures
//! DML, then **hot-only PK lookups before flush** (heaps still hold all rows —
//! fair merge-scan overhead). On PostgreSQL-only, **cold-id / hot+cold PK
//! lookups also run before `VACUUM FULL`** so they are not compared against a
//! freshly rewritten 10M heap. Managed sides flush older rows to zstd Parquet
//! (timing + peak RSS), time **cold-only** and **hot+cold (50/50)** after flush
//! (before hot-heap VACUUM), then VACUUM for the maintenance metric and compare
//! PostgreSQL heap/index sizes versus total hot+cold footprint.
//!
//! Timed INSERT always seeds into an empty table growing to `rows` on every
//! side — `hot_row_limit` does **not** make managed INSERT faster (flush has
//! not run yet). After the timed seed the harness probes PK bounds and logs
//! WAL bytes + pre-flush heap/index size. PostgreSQL-only pins a logical
//! slot (+ publication on the seed table) during the timed insert so WAL
//! recycle pressure matches managed async capture; without that pin, plain
//! PG looks artificially slower from segment recycle, not from less work.
//! Expect foreground insert ≈ identical or managed slightly slower once both
//! sides start the timed seed from the same recycled WAL baseline and retain
//! WAL the same way during seed (pg-only uses a temporary logical pin).
//! Mirror apply remains a separate catch-up row on managed.
//! Managed sizes always include `koldstore.<table>__cl` heap + indexes.
//!
//! Published runs isolate each column via `KOLDSTORE_STORAGE_SIDE=pg|async`
//! on a wiped + re-initdb pgrx data directory (see
//! `scripts/run-storage-comparison.sh --all-sides`).
//! `combined` keeps the interleaved dual-table smoke path for local debugging.

#[path = "../e2e/common/mod.rs"]
mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use koldstore_memory::matched_processes_rss_bytes;
use tokio_postgres::Client;

/// Local default keeps the harness usable; set `KOLDSTORE_STORAGE_ROWS=10000000`
/// for published RESULTS scale.
const DEFAULT_ROWS: i64 = 100_000;
const DEFAULT_HOT_LIMIT: i64 = 10_000;
const DEFAULT_DML_SAMPLE: i64 = 1_000;
const DEFAULT_INSERT_BATCH_ROWS: i64 = 100_000;
/// Untimed warm-up inserts before the timed seed (0 disables).
///
/// Default when unset: `min(rows, max(1_000_000, 5 * insert_batch_rows))` so
/// published 10M runs heat the server before measurement.
const DEFAULT_WARMUP_ROWS_SENTINEL: i64 = -1;
/// Point-lookup iterations counted toward throughput + p99.
const QUERY_LOOPS: usize = 400;
/// Untimed leading lookups discarded so EXPLAIN / first Parquet open / plan
/// cache do not dominate the steady-state sample (especially cold paths).
const QUERY_WARMUP_DISCARD: usize = 40;
/// Update/delete latency sample size when splitting the DML sample into batches.
const DEFAULT_DML_LATENCY_BATCH_ROWS: i64 = 1_000;

#[derive(Debug, Clone, Copy)]
struct Timing {
    elapsed: Duration,
    ops: i64,
    /// p99 of sampled op latencies (insert/update batch, or one PK lookup).
    p99_us: Option<f64>,
}

impl Timing {
    fn with_p99(elapsed: Duration, ops: i64, samples: &[Duration]) -> Self {
        Self {
            elapsed,
            ops,
            p99_us: percentile_us(samples, 0.99),
        }
    }

    fn per_op_us(self) -> f64 {
        if self.ops <= 0 {
            return 0.0;
        }
        self.elapsed.as_secs_f64() * 1_000_000.0 / self.ops as f64
    }

    fn ops_per_sec(self) -> f64 {
        if self.elapsed.is_zero() {
            return 0.0;
        }
        self.ops as f64 / self.elapsed.as_secs_f64()
    }
}

/// Nearest-rank percentile over sample durations, returned in microseconds.
fn percentile_us(samples: &[Duration], percentile: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((percentile.clamp(0.0, 1.0) * sorted.len() as f64).ceil() as usize).max(1);
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    Some(sorted[idx].as_secs_f64() * 1_000_000.0)
}

#[derive(Debug, Clone, Copy, Default)]
struct Sizes {
    /// User-table heap bytes (`pg_table_size`), excluding indexes.
    table_bytes: i64,
    /// User-table index bytes (`pg_indexes_size`).
    index_bytes: i64,
    /// Change-log mirror heap bytes (`koldstore.<table>__cl`), when measured.
    mirror_table_bytes: i64,
    /// Change-log mirror index bytes (PK + seq + tombstone), when measured.
    mirror_index_bytes: i64,
    /// Active cold Parquet `byte_size` sum from `koldstore.cold_segments`.
    cold_bytes: i64,
}

impl Sizes {
    /// PostgreSQL heap bytes charged to this side (user table + `__cl` when present).
    fn pg_table_bytes(self) -> i64 {
        self.table_bytes.saturating_add(self.mirror_table_bytes)
    }

    /// PostgreSQL index bytes charged to this side (user indexes + `__cl` indexes).
    fn pg_index_bytes(self) -> i64 {
        self.index_bytes.saturating_add(self.mirror_index_bytes)
    }

    fn pg_total_bytes(self) -> i64 {
        self.pg_table_bytes().saturating_add(self.pg_index_bytes())
    }
}

/// Heap live/dead tuple counters from `pg_stat_user_tables` after the DML workload.
#[derive(Debug, Clone, Copy)]
struct HeapStats {
    n_live_tup: i64,
    n_dead_tup: i64,
}

#[derive(Debug, Clone, Copy)]
struct SideMetrics {
    insert: Timing,
    update: Timing,
    delete: Timing,
    query_hot_only: Timing,
    /// After flush: alternating hot PK + cold PK lookups (50/50).
    query_hot_cold: Timing,
    /// After flush: cold PK only (`id = 1`).
    query_cold_only: Timing,
    /// Timed `VACUUM (FULL, ANALYZE)` after flush (smaller hot heap → less work).
    vacuum: Timing,
    /// Dead/live tuple pressure after DML, before flush.
    heap_after_workload: HeapStats,
    sizes: Sizes,
    async_catchup: Option<AsyncCatchup>,
}

#[derive(Debug, Clone, Copy)]
struct AsyncCatchup {
    insert: Timing,
    update: Timing,
    delete: Timing,
    restore: Timing,
}

/// Timed flush with peak cluster RSS sampled while the job runs.
#[derive(Debug, Clone, Copy)]
struct FlushMetrics {
    rows_flushed: i64,
    duration: Duration,
    /// Cluster RSS immediately before `flush_table`.
    before_rss_bytes: u64,
    /// Max cluster RSS observed during the flush window (polled).
    peak_rss_bytes: u64,
    /// Cluster RSS immediately after `flush_table` returns.
    after_rss_bytes: u64,
}

/// Which column this process measures. Published runs use an isolated side on
/// a fresh server; `Combined` is the interleaved dual-table smoke path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageSide {
    /// Plain PostgreSQL heap only (`postgres_only` column).
    Pg,
    /// Managed table with WAL-only mirror capture.
    Async,
    /// Interleaved baseline + managed on one server (smoke only).
    Combined,
}

fn parse_storage_side() -> Result<StorageSide> {
    let raw = std::env::var("KOLDSTORE_STORAGE_SIDE").unwrap_or_else(|_| "combined".to_string());
    match raw.as_str() {
        "pg" | "postgres" | "baseline" => Ok(StorageSide::Pg),
        "async" => Ok(StorageSide::Async),
        "combined" | "both" => Ok(StorageSide::Combined),
        other => {
            anyhow::bail!("KOLDSTORE_STORAGE_SIDE must be pg|async|combined (got {other})")
        }
    }
}

fn side_mode_label(side: StorageSide) -> &'static str {
    match side {
        StorageSide::Pg => "pg",
        StorageSide::Async => "async",
        StorageSide::Combined => "combined",
    }
}

#[tokio::test]
async fn pg_vs_koldstore_storage_and_speed_comparison() -> Result<()> {
    common::require_pgrx_server().await?;
    let rows = env_i64("KOLDSTORE_STORAGE_ROWS", DEFAULT_ROWS);
    let hot_limit = env_i64("KOLDSTORE_STORAGE_HOT_LIMIT", DEFAULT_HOT_LIMIT)
        .clamp(1, rows.saturating_sub(1).max(1));
    let dml_sample = env_i64("KOLDSTORE_STORAGE_DML_SAMPLE", DEFAULT_DML_SAMPLE).clamp(1, rows);
    let insert_batch_rows = env_i64(
        "KOLDSTORE_STORAGE_INSERT_BATCH_ROWS",
        DEFAULT_INSERT_BATCH_ROWS,
    )
    .clamp(1, rows);
    let warmup_rows = resolve_warmup_rows(rows, insert_batch_rows);
    let side = parse_storage_side()?;
    let max_rows_per_file = env_i64(
        "KOLDSTORE_STORAGE_MAX_ROWS_PER_FILE",
        (rows / 10).max(1_000),
    );
    // One flush_table job is capped at MAX_CATCHUP_WAVES_PER_JOB (64) ×
    // max_rows_per_flush. Default product max_rows_per_flush is 10k → 640k
    // rows/call, which cannot drain a 10M RESULTS run. Size the wave for the
    // expected cold excess so one call finishes the policy drain (fewer waves
    // also cuts finalize/manifest overhead — the main flush wall-clock cost).
    let max_rows_per_flush = env_i64(
        "KOLDSTORE_STORAGE_MAX_ROWS_PER_FLUSH",
        rows.saturating_sub(hot_limit).max(max_rows_per_file).max(1),
    );

    let target = common::local_pg_matrix()
        .into_iter()
        .next()
        .context("no local pg target configured")?;
    let db = common::TestDb::start(target, "storage_cmp").await?;
    let dbname: String = db
        .client
        .query_one("SELECT current_database()", &[])
        .await?
        .get(0);

    // Warm the extension library so planner hooks/GUCs are registered.
    let _ = db
        .client
        .query_one("SELECT koldstore_version()", &[])
        .await?;

    common::log_always(format!(
        "storage_cmp: side={} rows={} hot_limit={} dml_sample={} insert_batch_rows={} warmup_rows={} max_rows_per_flush={}",
        side_mode_label(side),
        rows,
        hot_limit,
        dml_sample,
        insert_batch_rows,
        warmup_rows,
        max_rows_per_flush
    ));

    match side {
        StorageSide::Pg => {
            let table = format!("{}_baseline", db.schema);
            let relation = format!("{}.{}", db.schema, table);
            apply_schema_sql(&db.client, &db.schema, &table).await?;
            run_pg_only_body(
                &db,
                &relation,
                &table,
                rows,
                hot_limit,
                dml_sample,
                insert_batch_rows,
                max_rows_per_file,
                warmup_rows,
            )
            .await
        }
        StorageSide::Async => {
            let table = format!("{}_managed", db.schema);
            let relation = format!("{}.{}", db.schema, table);
            apply_schema_sql(&db.client, &db.schema, &table).await?;
            {
                let _step = common::log_step_always("storage_cmp: manage_table");
                manage_with_hot_limit(
                    &db.client,
                    &db.storage_name,
                    &relation,
                    hot_limit,
                    1,
                    max_rows_per_file,
                    max_rows_per_flush,
                )
                .await?;
            }
            // Warm-up / large catch-up can retain >1 GiB slot WAL. Disable the
            // lab health alarm before warm-up; apply remains enabled.
            disable_async_retained_wal_health_threshold_for_benchmark(&db.client, &dbname).await?;
            // Warm-up before pinning the worker off: manage_table needs the
            // worker GUC enabled for activation on the throwaway table.
            warm_up_before_timed_seed(
                &db.client,
                &db.schema,
                &table,
                Some(&db.storage_name),
                "async",
                warmup_rows,
                insert_batch_rows,
                hot_limit,
                max_rows_per_file,
                max_rows_per_flush,
            )
            .await?;
            let worker_guc_pinned = disable_async_worker_for_benchmark(&db.client, &dbname).await?;
            let result = run_managed_only_body(
                &db,
                &relation,
                &table,
                rows,
                hot_limit,
                dml_sample,
                insert_batch_rows,
                max_rows_per_file,
                warmup_rows,
            )
            .await;
            if worker_guc_pinned {
                reset_async_worker_guc(&db.client, &dbname).await?;
            }
            result
        }
        StorageSide::Combined => {
            // Mirror tables are `koldstore.<unqualified>__cl`, so table names
            // must be unique across schemas in the shared E2E database.
            let baseline_table = format!("{}_baseline", db.schema);
            let managed_table = format!("{}_managed", db.schema);
            let baseline = format!("{}.{}", db.schema, baseline_table);
            let managed = format!("{}.{}", db.schema, managed_table);

            apply_schema_sql(&db.client, &db.schema, &baseline_table).await?;
            apply_schema_sql(&db.client, &db.schema, &managed_table).await?;

            {
                let _step = common::log_step_always("storage_cmp: manage_table");
                manage_with_hot_limit(
                    &db.client,
                    &db.storage_name,
                    &managed,
                    hot_limit,
                    1,
                    max_rows_per_file,
                    max_rows_per_flush,
                )
                .await?;
            }
            let worker_guc_pinned = disable_async_worker_for_benchmark(&db.client, &dbname).await?;
            let result = run_storage_comparison_body(
                &db,
                &baseline,
                &managed,
                &managed_table,
                rows,
                hot_limit,
                dml_sample,
                insert_batch_rows,
                max_rows_per_file,
            )
            .await;
            if worker_guc_pinned {
                reset_async_worker_guc(&db.client, &dbname).await?;
            }
            result
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_pg_only_body(
    db: &common::TestDb,
    baseline: &str,
    baseline_table: &str,
    rows: i64,
    hot_limit: i64,
    dml_sample: i64,
    insert_batch_rows: i64,
    max_rows_per_file: i64,
    warmup_rows: i64,
) -> Result<()> {
    warm_up_before_timed_seed(
        &db.client,
        &db.schema,
        baseline_table,
        None,
        "pg",
        warmup_rows,
        insert_batch_rows,
        hot_limit,
        max_rows_per_file,
        max_rows_per_file,
    )
    .await?;
    common::log_always(format!(
        "storage_cmp: seeding {rows} rows on PostgreSQL only (insert_batch_rows={insert_batch_rows})"
    ));
    // Equalize with managed: recycle warm-up WAL to the minimum *before* the
    // retention pin, then pin at tip so both sides start timed insert with the
    // same low wal_files baseline and the same no-recycle-during-seed shape.
    prepare_timed_insert_wal_baseline(&db.client, "pg-only", /*drain_async=*/ false).await?;
    pin_wal_retention_for_fair_insert(&db.client, baseline).await?;
    let insert = {
        let insert_result = async {
            checkpoint_before_timing(&db.client, "pg-only inserts").await?;
            let _step = common::log_step_always(format!(
                "storage_cmp: batched pg-only inserts ({rows} rows)"
            ));
            time_batched_inserts(&db.client, baseline, rows, insert_batch_rows).await
        }
        .await;
        match &insert_result {
            Ok(_) => {}
            Err(_) => {
                let _ = unpin_wal_retention_for_fair_insert(&db.client).await;
            }
        }
        let insert = insert_result?;
        verify_timed_seed(&db.client, baseline, rows, "pg-only").await?;
        unpin_wal_retention_for_fair_insert(&db.client).await?;
        insert
    };

    checkpoint_before_timing(&db.client, "pg-only update").await?;
    let update = {
        let _step = common::log_step_always(format!(
            "storage_cmp: update pg-only sample ({dml_sample} rows)"
        ));
        time_update(&db.client, baseline, rows - dml_sample + 1, dml_sample).await?
    };

    let delete_start = rows - dml_sample + 1;
    let delete_end = rows;
    checkpoint_before_timing(&db.client, "pg-only delete").await?;
    let delete = {
        let _step = common::log_step_always("storage_cmp: delete pg-only sample");
        time_delete(&db.client, baseline, delete_start, delete_end).await?
    };
    {
        let _step = common::log_step_always(format!(
            "storage_cmp: restore delete sample ({dml_sample} rows)"
        ));
        time_insert(&db.client, baseline, delete_start, dml_sample).await?;
    }

    let _ = db
        .client
        .execute("SELECT pg_stat_force_next_flush()", &[])
        .await;
    let heap = heap_stats(&db.client, baseline).await?;

    let hot_id = rows;
    let cold_id = 1_i64;
    // All PK phases before VACUUM FULL: cold-id is still on the full heap, same
    // post-DML state as hot-only. Measuring after FULL vacuum would compare a
    // freshly rewritten 10M table to managed Parquet and inflate the gap.
    let query_hot_only = {
        let _step = common::log_step_always("storage_cmp: time pg-only hot PK lookups");
        time_point_queries(&db.client, baseline, hot_id).await?
    };
    let query_cold_only = {
        let _step = common::log_step_always("storage_cmp: time pg-only cold-id PK lookups");
        time_point_queries(&db.client, baseline, cold_id).await?
    };
    let query_hot_cold = {
        let _step = common::log_step_always("storage_cmp: time pg-only mixed hot+cold PK lookups");
        time_mixed_hot_cold_queries(&db.client, baseline, hot_id, cold_id).await?
    };

    // No flush on unmanaged heap; still time VACUUM FULL on the full table so
    // maintenance cost is comparable to managed post-flush VACUUM.
    let vacuum = {
        let _step = common::log_step_always("storage_cmp: VACUUM FULL pg-only (~full heap)");
        time_vacuum_full(&db.client, baseline).await?
    };
    {
        let _step = common::log_step_always("storage_cmp: REINDEX pg-only");
        reindex_relation(&db.client, baseline).await?;
    }
    let sizes = relation_sizes(&db.client, baseline, None).await?;

    let metrics = SideMetrics {
        insert,
        update,
        delete,
        query_hot_only,
        query_hot_cold,
        query_cold_only,
        vacuum,
        heap_after_workload: heap,
        sizes,
        async_catchup: None,
    };

    print_comparison_table(
        rows,
        hot_limit,
        dml_sample,
        insert_batch_rows,
        max_rows_per_file,
        warmup_rows,
        None,
        Some(metrics),
        None,
        "pg",
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_managed_only_body(
    db: &common::TestDb,
    managed: &str,
    managed_table: &str,
    rows: i64,
    hot_limit: i64,
    dml_sample: i64,
    insert_batch_rows: i64,
    max_rows_per_file: i64,
    warmup_rows: i64,
) -> Result<()> {
    assert_async_worker_disabled_for_benchmark(&db.client).await?;
    db.client
        .batch_execute(&format!(
            "ALTER TABLE koldstore.{managed_table}__cl SET (autovacuum_enabled = false)"
        ))
        .await
        .context("disable autovacuum on benchmark mirror")?;
    common::log_always(format!(
        "storage_cmp: seeding {rows} rows on managed-only (insert_batch_rows={insert_batch_rows}, hot_row_limit={hot_limit}, max_rows_per_file={max_rows_per_file}, warmup_rows={warmup_rows})"
    ));
    // Drain warm-up apply to the tip, then recycle to the same low wal_files
    // baseline as PostgreSQL-only before the timed seed retains WAL again.
    prepare_timed_insert_wal_baseline(&db.client, "managed-only", /*drain_async=*/ true).await?;
    checkpoint_before_timing(&db.client, "managed-only inserts").await?;
    let insert = {
        let _step = common::log_step_always(format!(
            "storage_cmp: batched managed inserts ({rows} rows)"
        ));
        time_batched_inserts(&db.client, managed, rows, insert_batch_rows).await?
    };
    // Seed check before catch-up: foreground insert must have written the full
    // heap before mirror apply; catch-up must not be what "creates" the rows.
    verify_timed_seed(&db.client, managed, rows, "managed-only").await?;
    let insert_catchup = async_catchup(&db.client, rows).await?;

    checkpoint_before_timing(&db.client, "managed update").await?;
    let update = {
        let _step = common::log_step_always(format!(
            "storage_cmp: update managed sample ({dml_sample} rows)"
        ));
        time_update(&db.client, managed, rows - dml_sample + 1, dml_sample).await?
    };
    let update_catchup = async_catchup(&db.client, dml_sample).await?;

    let delete_start = rows - dml_sample + 1;
    let delete_end = rows;
    checkpoint_before_timing(&db.client, "managed delete").await?;
    let delete = {
        let _step = common::log_step_always("storage_cmp: delete managed sample");
        time_delete(&db.client, managed, delete_start, delete_end).await?
    };
    let delete_catchup = async_catchup(&db.client, dml_sample).await?;
    {
        let _step = common::log_step_always(format!(
            "storage_cmp: restore delete sample ({dml_sample} rows)"
        ));
        time_insert(&db.client, managed, delete_start, dml_sample).await?;
    }
    let restore_catchup = async_catchup(&db.client, dml_sample).await?;

    let _ = db
        .client
        .execute("SELECT pg_stat_force_next_flush()", &[])
        .await;
    let heap = heap_stats(&db.client, managed).await?;

    let hot_id = rows;
    let cold_id = 1_i64;
    {
        let _step = common::log_step_always("storage_cmp: pre-flush hot-only PK lookups");
        let plan_pre_flush = common::explain(
            &db.client,
            &format!("SELECT id, account_id, event_type FROM {managed} WHERE id = {hot_id}"),
        )
        .await?;
        common::assert_managed_read_plan(&plan_pre_flush)?;
        anyhow::ensure!(
            !plan_pre_flush.contains("Custom Scan (KoldMergeScan)")
                || plan_pre_flush.contains("Parquet Segments Opened: 0")
                || plan_pre_flush.contains("Parquet Segments Planned: 0"),
            "pre-flush PK lookup must not open Parquet (no cold yet), got:\n{plan_pre_flush}"
        );
    }
    let query_hot_only = {
        let _step = common::log_step_always("storage_cmp: time managed hot PK lookups");
        time_point_queries(&db.client, managed, hot_id).await?
    };

    let flush = {
        let _step = common::log_step_always(format!(
            "storage_cmp: flush_table until hot_rows <= {hot_limit} (expect ~{} cold rows)",
            rows.saturating_sub(hot_limit)
        ));
        flush_until_hot_limit(&db.client, managed, db.target.port, hot_limit).await?
    };
    anyhow::ensure!(
        flush.rows_flushed > 0,
        "expected KoldStore flush to move rows cold (hot_limit={hot_limit}, rows={rows})"
    );
    common::log_always(format!(
        "storage_cmp: flush_table returned rows_flushed={} duration={} peak_rss={}",
        flush.rows_flushed,
        format_duration(flush.duration),
        format_bytes(flush.peak_rss_bytes as i64),
    ));

    let status = common::describe_table(&db.client, managed).await?;
    anyhow::ensure!(
        status.hot_rows > 0,
        "expected hot rows to remain after policy flush, got {status:?}"
    );
    anyhow::ensure!(
        status.hot_rows <= hot_limit,
        "expected hot_rows <= hot_limit after flush (hot_rows={}, hot_limit={hot_limit}, cold={})",
        status.hot_rows,
        status.cold_row_count
    );
    anyhow::ensure!(
        status.cold_row_count > 0,
        "expected cold rows after flush, got {status:?}"
    );
    common::log_always(format!(
        "storage_cmp: after flush hot={} cold={} mirror={}",
        status.hot_rows, status.cold_row_count, status.mirror_rows
    ));

    {
        let _step = common::log_step_always("storage_cmp: post-flush cold-only PK lookups");
        let plan_cold = common::explain(
            &db.client,
            &format!("SELECT id, account_id, event_type FROM {managed} WHERE id = {cold_id}"),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan_cold)?;
        common::assert_kold_merge_scan_cold_reads(&plan_cold, "manifest.json", 1)?;
        anyhow::ensure!(
            !plan_cold.contains("Parquet Segments Opened: 0")
                && !plan_cold.contains("Parquet Segments Planned: 0"),
            "cold-only PK lookup should open at least one Parquet segment, got:\n{plan_cold}"
        );
    }
    // Time cold / hot+cold after flush but before hot-heap VACUUM so the PG
    // baseline (full heap, pre-VACUUM) and managed Parquet path are not mixed
    // with a post-FULL-vacuum heap rewrite on the unmanaged side.
    let query_cold_only = {
        let _step = common::log_step_always("storage_cmp: time managed cold-only PK lookups");
        time_point_queries(&db.client, managed, cold_id).await?
    };
    let query_hot_cold = {
        let _step = common::log_step_always("storage_cmp: time managed mixed hot+cold PK lookups");
        time_mixed_hot_cold_queries(&db.client, managed, hot_id, cold_id).await?
    };

    let vacuum = {
        let _step = common::log_step_always("storage_cmp: VACUUM FULL managed (hot heap)");
        time_vacuum_full(&db.client, managed).await?
    };
    {
        let _step = common::log_step_always("storage_cmp: REINDEX + vacuum mirror");
        reindex_relation(&db.client, managed).await?;
        if let Some(mirror) = mirror_relation(managed) {
            time_vacuum_full(&db.client, &mirror).await?;
            reindex_relation(&db.client, &mirror).await?;
        }
    }

    let sizes = relation_sizes(&db.client, managed, Some(managed)).await?;
    anyhow::ensure!(
        sizes.mirror_table_bytes > 0,
        "expected change-log mirror heap bytes after flush, got {sizes:?}"
    );
    anyhow::ensure!(
        sizes.mirror_index_bytes > 0,
        "expected change-log mirror index bytes after flush, got {sizes:?}"
    );
    anyhow::ensure!(
        sizes.cold_bytes > 0,
        "expected positive cold Parquet bytes after flush"
    );

    let metrics = SideMetrics {
        insert,
        update,
        delete,
        query_hot_only,
        query_hot_cold,
        query_cold_only,
        vacuum,
        heap_after_workload: heap,
        sizes,
        async_catchup: Some(AsyncCatchup {
            insert: insert_catchup,
            update: update_catchup,
            delete: delete_catchup,
            restore: restore_catchup,
        }),
    };

    print_comparison_table(
        rows,
        hot_limit,
        dml_sample,
        insert_batch_rows,
        max_rows_per_file,
        warmup_rows,
        Some(flush),
        None,
        Some(metrics),
        "async",
    )?;

    let final_status = {
        let _step =
            common::log_step_always("storage_cmp: verify hot+cold coverage via describe_table");
        common::describe_table(&db.client, managed).await?
    };
    anyhow::ensure!(
        final_status.hot_rows > 0,
        "expected hot rows after flush, got {final_status:?}"
    );
    anyhow::ensure!(
        final_status.cold_row_count > 0,
        "expected cold rows after flush, got {final_status:?}"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_storage_comparison_body(
    db: &common::TestDb,
    baseline: &str,
    managed: &str,
    managed_table: &str,
    rows: i64,
    hot_limit: i64,
    dml_sample: i64,
    insert_batch_rows: i64,
    max_rows_per_file: i64,
) -> Result<()> {
    assert_async_worker_disabled_for_benchmark(&db.client).await?;
    // Apply the source tables' benchmark-only autovacuum control to the
    // generated mirror. Otherwise a long async catch-up can launch mirror
    // maintenance during the next timed phase.
    db.client
        .batch_execute(&format!(
            "ALTER TABLE koldstore.{managed_table}__cl SET (autovacuum_enabled = false)"
        ))
        .await
        .context("disable autovacuum on benchmark mirror")?;
    common::log_always(format!(
        "storage_cmp: seeding {rows} rows (insert_batch_rows={insert_batch_rows}, hot_row_limit={hot_limit}, max_rows_per_file={max_rows_per_file})"
    ));
    checkpoint_before_timing(&db.client, "interleaved inserts").await?;
    let (baseline_insert, managed_insert) = {
        let _step = common::log_step_always(format!(
            "storage_cmp: interleaved baseline/managed inserts ({rows} rows each)"
        ));
        time_interleaved_inserts(&db.client, baseline, managed, rows, insert_batch_rows).await?
    };
    let insert_catchup = async_catchup(&db.client, rows).await?;

    checkpoint_before_timing(&db.client, "baseline update").await?;
    let baseline_update = {
        let _step = common::log_step_always(format!(
            "storage_cmp: update baseline sample ({dml_sample} rows)"
        ));
        time_update(&db.client, baseline, rows - dml_sample + 1, dml_sample).await?
    };
    checkpoint_before_timing(&db.client, "managed update").await?;
    let managed_update = {
        let _step = common::log_step_always(format!(
            "storage_cmp: update managed sample ({dml_sample} rows)"
        ));
        time_update(&db.client, managed, rows - dml_sample + 1, dml_sample).await?
    };
    let update_catchup = async_catchup(&db.client, dml_sample).await?;

    // Delete from the high end of the seeded range, then re-insert the same
    // keys before flush. Seeding a disjoint range left `dml_sample` tombstones
    // in the mirror; flush-by-oldest-seq then evacuated live hot rows while
    // retaining tombstones whenever `dml_sample` approached `rows`.
    let delete_start = rows - dml_sample + 1;
    let delete_end = rows;
    checkpoint_before_timing(&db.client, "baseline delete").await?;
    let baseline_delete = {
        let _step = common::log_step_always("storage_cmp: delete baseline sample");
        time_delete(&db.client, baseline, delete_start, delete_end).await?
    };
    checkpoint_before_timing(&db.client, "managed delete").await?;
    let managed_delete = {
        let _step = common::log_step_always("storage_cmp: delete managed sample");
        time_delete(&db.client, managed, delete_start, delete_end).await?
    };
    let delete_catchup = async_catchup(&db.client, dml_sample).await?;
    {
        let _step = common::log_step_always(format!(
            "storage_cmp: restore delete sample ({dml_sample} rows each side)"
        ));
        time_insert(&db.client, baseline, delete_start, dml_sample).await?;
        time_insert(&db.client, managed, delete_start, dml_sample).await?;
    }
    let restore_catchup = async_catchup(&db.client, dml_sample).await?;

    // Snapshot bloat / autovacuum pressure after DML, before flush reclaims space.
    // Force the stats collector so n_dead_tup reflects this backend's DML.
    let _ = db
        .client
        .execute("SELECT pg_stat_force_next_flush()", &[])
        .await;
    let baseline_heap = heap_stats(&db.client, baseline).await?;
    let managed_heap = heap_stats(&db.client, managed).await?;

    // Fair hot-only comparison: measure PK lookups while every row is still in
    // the PostgreSQL heap on both sides (before any flush). Post-flush "hot"
    // would compare a ~hot_limit managed heap to a full baseline heap.
    let hot_id = rows;
    let cold_id = 1_i64;

    {
        let _step = common::log_step_always("storage_cmp: pre-flush hot-only PK lookups");
        let plan_pre_flush = common::explain(
            &db.client,
            &format!("SELECT id, account_id, event_type FROM {managed} WHERE id = {hot_id}"),
        )
        .await?;
        common::assert_managed_read_plan(&plan_pre_flush)?;
        anyhow::ensure!(
            !plan_pre_flush.contains("Custom Scan (KoldMergeScan)")
                || plan_pre_flush.contains("Parquet Segments Opened: 0")
                || plan_pre_flush.contains("Parquet Segments Planned: 0"),
            "pre-flush PK lookup must not open Parquet (no cold yet), got:\n{plan_pre_flush}"
        );
        assert_point_row_matches(&db.client, baseline, managed, hot_id).await?;
        assert_point_row_matches(&db.client, baseline, managed, cold_id).await?;
    }

    let baseline_hot = {
        let _step = common::log_step_always("storage_cmp: time baseline hot PK lookups");
        time_point_queries(&db.client, baseline, hot_id).await?
    };
    let managed_hot = {
        let _step = common::log_step_always("storage_cmp: time managed hot PK lookups");
        time_point_queries(&db.client, managed, hot_id).await?
    };

    let flush = {
        let _step = common::log_step_always(format!(
            "storage_cmp: flush_table until hot_rows <= {hot_limit} (expect ~{} cold rows)",
            rows.saturating_sub(hot_limit)
        ));
        flush_until_hot_limit(&db.client, managed, db.target.port, hot_limit).await?
    };
    anyhow::ensure!(
        flush.rows_flushed > 0,
        "expected KoldStore flush to move rows cold (hot_limit={hot_limit}, rows={rows})"
    );
    common::log_always(format!(
        "storage_cmp: flush_table returned rows_flushed={} duration={} peak_rss={}",
        flush.rows_flushed,
        format_duration(flush.duration),
        format_bytes(flush.peak_rss_bytes as i64),
    ));

    let status = common::describe_table(&db.client, managed).await?;
    anyhow::ensure!(
        status.hot_rows > 0,
        "expected hot rows to remain after policy flush, got {status:?}"
    );
    anyhow::ensure!(
        status.hot_rows <= hot_limit,
        "expected hot_rows <= hot_limit after flush (hot_rows={}, hot_limit={hot_limit}, cold={})",
        status.hot_rows,
        status.cold_row_count
    );
    anyhow::ensure!(
        status.cold_row_count > 0,
        "expected cold rows after flush, got {status:?}"
    );
    common::log_always(format!(
        "storage_cmp: after flush hot={} cold={} mirror={}",
        status.hot_rows, status.cold_row_count, status.mirror_rows
    ));

    {
        let _step = common::log_step_always("storage_cmp: post-flush cold-only PK lookups");
        let plan_cold = common::explain(
            &db.client,
            &format!("SELECT id, account_id, event_type FROM {managed} WHERE id = {cold_id}"),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan_cold)?;
        common::assert_kold_merge_scan_cold_reads(&plan_cold, "manifest.json", 1)?;
        anyhow::ensure!(
            !plan_cold.contains("Parquet Segments Opened: 0")
                && !plan_cold.contains("Parquet Segments Planned: 0"),
            "cold-only PK lookup should open at least one Parquet segment, got:\n{plan_cold}"
        );

        // Correctness after flush: cold-flushed and still-hot PKs must match baseline.
        assert_point_row_matches(&db.client, baseline, managed, hot_id).await?;
        assert_point_row_matches(&db.client, baseline, managed, cold_id).await?;
        let mid_cold_id = (hot_limit / 2).max(1);
        assert_point_row_matches(&db.client, baseline, managed, mid_cold_id).await?;
    }

    // Baseline cold/hot+cold on the full heap before VACUUM FULL; managed on
    // Parquet after flush before hot-heap VACUUM — same contract as isolated sides.
    let baseline_cold_only = {
        let _step = common::log_step_always("storage_cmp: time baseline cold-only PK lookups");
        time_point_queries(&db.client, baseline, cold_id).await?
    };
    let managed_cold_only = {
        let _step = common::log_step_always("storage_cmp: time managed cold-only PK lookups");
        time_point_queries(&db.client, managed, cold_id).await?
    };
    let baseline_hot_cold = {
        let _step = common::log_step_always("storage_cmp: time baseline mixed hot+cold PK lookups");
        time_mixed_hot_cold_queries(&db.client, baseline, hot_id, cold_id).await?
    };
    let managed_hot_cold = {
        let _step = common::log_step_always("storage_cmp: time managed mixed hot+cold PK lookups");
        time_mixed_hot_cold_queries(&db.client, managed, hot_id, cold_id).await?
    };

    // After flush the managed heap is smaller; time VACUUM FULL as the
    // maintenance-cost comparison, then REINDEX so size numbers are clean.
    let baseline_vacuum = {
        let _step = common::log_step_always("storage_cmp: VACUUM FULL baseline (~full heap)");
        time_vacuum_full(&db.client, baseline).await?
    };
    let managed_vacuum = {
        let _step = common::log_step_always("storage_cmp: VACUUM FULL managed (hot heap)");
        time_vacuum_full(&db.client, managed).await?
    };
    {
        let _step = common::log_step_always("storage_cmp: REINDEX + vacuum mirror");
        reindex_relation(&db.client, baseline).await?;
        reindex_relation(&db.client, managed).await?;
        if let Some(mirror) = mirror_relation(managed) {
            time_vacuum_full(&db.client, &mirror).await?;
            reindex_relation(&db.client, &mirror).await?;
        }
    }

    let baseline_sizes = relation_sizes(&db.client, baseline, None).await?;
    let managed_sizes = relation_sizes(&db.client, managed, Some(managed)).await?;
    anyhow::ensure!(
        managed_sizes.mirror_table_bytes > 0,
        "expected change-log mirror heap bytes after flush, got {managed_sizes:?}"
    );
    anyhow::ensure!(
        managed_sizes.mirror_index_bytes > 0,
        "expected change-log mirror index bytes after flush, got {managed_sizes:?}"
    );

    let baseline_metrics = SideMetrics {
        insert: baseline_insert,
        update: baseline_update,
        delete: baseline_delete,
        query_hot_only: baseline_hot,
        query_hot_cold: baseline_hot_cold,
        query_cold_only: baseline_cold_only,
        vacuum: baseline_vacuum,
        heap_after_workload: baseline_heap,
        sizes: baseline_sizes,
        async_catchup: None,
    };
    let managed_metrics = SideMetrics {
        insert: managed_insert,
        update: managed_update,
        delete: managed_delete,
        query_hot_only: managed_hot,
        query_hot_cold: managed_hot_cold,
        query_cold_only: managed_cold_only,
        vacuum: managed_vacuum,
        heap_after_workload: managed_heap,
        sizes: managed_sizes,
        async_catchup: Some(AsyncCatchup {
            insert: insert_catchup,
            update: update_catchup,
            delete: delete_catchup,
            restore: restore_catchup,
        }),
    };

    print_comparison_table(
        rows,
        hot_limit,
        dml_sample,
        insert_batch_rows,
        max_rows_per_file,
        0, // combined smoke path: no warm-up accounting
        Some(flush),
        Some(baseline_metrics),
        Some(managed_metrics),
        "combined",
    )?;

    anyhow::ensure!(
        managed_sizes.pg_table_bytes() < baseline_sizes.pg_table_bytes(),
        "expected KoldStore PostgreSQL table footprint (hot+__cl) smaller after flush: managed={} baseline={}",
        managed_sizes.pg_table_bytes(),
        baseline_sizes.pg_table_bytes()
    );
    anyhow::ensure!(
        managed_sizes.pg_index_bytes() < baseline_sizes.pg_index_bytes(),
        "expected KoldStore PostgreSQL index footprint (hot+__cl) smaller after flush: managed={} baseline={}",
        managed_sizes.pg_index_bytes(),
        baseline_sizes.pg_index_bytes()
    );
    anyhow::ensure!(
        managed_sizes.cold_bytes > 0,
        "expected positive cold Parquet bytes after flush"
    );

    // Point lookups above already checked correctness. Avoid counting the
    // managed relation: even `ONLY` still routes through KoldMergeScan, and
    // disabling merge scan rejects managed SELECTs. Catalog counters are enough
    // to confirm flush produced hot + cold coverage.
    let final_status = {
        let _step =
            common::log_step_always("storage_cmp: verify hot+cold coverage via describe_table");
        common::describe_table(&db.client, managed).await?
    };
    anyhow::ensure!(
        final_status.hot_rows > 0,
        "expected hot rows after flush, got {final_status:?}"
    );
    anyhow::ensure!(
        final_status.cold_row_count > 0,
        "expected cold rows after flush, got {final_status:?}"
    );
    common::log_always(format!(
        "storage_cmp: coverage ok describe_hot={} cold={} (seeded={rows})",
        final_status.hot_rows, final_status.cold_row_count
    ));

    Ok(())
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Resolves warm-up row count: explicit env, else a scale-aware default, else 0.
fn resolve_warmup_rows(rows: i64, insert_batch_rows: i64) -> i64 {
    let configured = env_i64(
        "KOLDSTORE_STORAGE_WARMUP_ROWS",
        DEFAULT_WARMUP_ROWS_SENTINEL,
    );
    let warmup = if configured == DEFAULT_WARMUP_ROWS_SENTINEL {
        // Heat shared buffers / WAL / disk before timing. Cap at timed row count.
        rows.min((insert_batch_rows.saturating_mul(5)).max(1_000_000))
    } else {
        configured
    };
    warmup.clamp(0, rows)
}

/// Untimed warm-up on a throwaway table, then drop it so the timed table stays empty.
///
/// This rejects cold-start insert skew (first heavy write after install/start)
/// while keeping the measured relation empty for a clean 10M seed + flush.
#[allow(clippy::too_many_arguments)]
async fn warm_up_before_timed_seed(
    client: &Client,
    schema: &str,
    main_table: &str,
    storage: Option<&str>,
    side_label: &str,
    warmup_rows: i64,
    insert_batch_rows: i64,
    hot_limit: i64,
    max_rows_per_file: i64,
    max_rows_per_flush: i64,
) -> Result<()> {
    if warmup_rows <= 0 {
        common::log_always(format!(
            "storage_cmp: warm-up skipped for {side_label} (warmup_rows=0)"
        ));
        return Ok(());
    }

    let warmup_table = format!("{main_table}_warmup");
    let warmup_relation = format!("{schema}.{warmup_table}");
    let _step = common::log_step_always(format!(
        "storage_cmp: warm-up {side_label} ({warmup_rows} rows into {warmup_relation}, untimed)"
    ));

    apply_schema_sql(client, schema, &warmup_table).await?;
    if let Some(storage_name) = storage {
        manage_with_hot_limit(
            client,
            storage_name,
            &warmup_relation,
            hot_limit,
            1,
            max_rows_per_file,
            max_rows_per_flush,
        )
        .await?;
        client
            .batch_execute(&format!(
                "ALTER TABLE koldstore.{warmup_table}__cl SET (autovacuum_enabled = false)"
            ))
            .await
            .context("disable autovacuum on warm-up mirror")?;
    }

    let started = Instant::now();
    let _ = time_batched_inserts(client, &warmup_relation, warmup_rows, insert_batch_rows).await?;
    if storage.is_some() {
        // Drain whatever the launcher/worker already applied plus the remainder.
        // Do not require an exact applied count — warm-up may race a live worker.
        async_catchup_drain(client).await?;
    }
    common::log_always(format!(
        "storage_cmp: warm-up inserts finished in {:.3}s",
        started.elapsed().as_secs_f64()
    ));

    // DROP removes managed catalog/mirror via the extension ProcessUtility path.
    client
        .batch_execute(&format!("DROP TABLE {warmup_relation}"))
        .await
        .with_context(|| format!("drop warm-up table {warmup_relation}"))?;
    checkpoint_before_timing(client, &format!("{side_label} after warm-up")).await?;
    common::log_always(format!(
        "storage_cmp: warm-up complete; timed table {schema}.{main_table} is empty"
    ));
    Ok(())
}

fn mirror_relation(managed: &str) -> Option<String> {
    managed
        .rsplit('.')
        .next()
        .map(|table| format!("koldstore.{table}__cl"))
}

fn schema_sql_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("schema.sql")
}

async fn apply_schema_sql(client: &Client, schema: &str, table: &str) -> Result<()> {
    let template = std::fs::read_to_string(schema_sql_path())
        .with_context(|| format!("read {}", schema_sql_path().display()))?;
    let sql = template
        .replace("{{schema}}", schema)
        .replace("{{table}}", table);
    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("apply schema.sql for {schema}.{table}"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn manage_with_hot_limit(
    client: &Client,
    storage: &str,
    relation: &str,
    hot_row_limit: i64,
    min_flush_rows: i64,
    max_rows_per_file: i64,
    max_rows_per_flush: i64,
) -> Result<()> {
    // auto_flush=false so background DB-worker waves don't steal the timed
    // flush_table measurement (and leave rows_flushed=0 at the explicit call).
    client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name        => $1::text::regclass,
              storage           => $2,
              hot_row_limit     => $3,
              min_flush_rows    => $4,
              max_rows_per_file => $5,
              migration_order_by => 'id',
              compression       => 'zstd',
              auto_flush        => false
            )
            "#,
            &[
                &relation,
                &storage,
                &hot_row_limit,
                &min_flush_rows,
                &max_rows_per_file,
            ],
        )
        .await
        .with_context(|| format!("manage_table {relation}"))?;
    // manage_table does not expose max_rows_per_flush; set it after manage so a
    // single flush_table can drain to hot_row_limit (product default is 10k/wave
    // × 64 waves = 640k max per call).
    client
        .execute(
            &format!(
                "ALTER TABLE {relation} SET (koldstore_max_rows_per_flush = {max_rows_per_flush})"
            ),
            &[],
        )
        .await
        .with_context(|| {
            format!("set koldstore_max_rows_per_flush={max_rows_per_flush} on {relation}")
        })?;
    Ok(())
}

async fn async_catchup(client: &Client, expected_changes: i64) -> Result<Timing> {
    // Stop a launcher-respawned applier so this fence owns the apply work.
    let _ = terminate_async_workers_until_idle(client).await;
    let started = Instant::now();
    let applied: i64 = client
        .query_one("SELECT koldstore.wait_for_async_mirror()", &[])
        .await?
        .get(0);
    anyhow::ensure!(
        applied == expected_changes,
        "async mirror applied {applied} changes, expected {expected_changes}"
    );
    let acknowledged: i64 = client
        .query_one("SELECT koldstore.wait_for_async_mirror()", &[])
        .await?
        .get(0);
    anyhow::ensure!(
        acknowledged == 0,
        "async mirror acknowledgement replayed {acknowledged} changes"
    );
    Ok(Timing {
        elapsed: started.elapsed(),
        ops: expected_changes,
        p99_us: None,
    })
}

/// Drain async mirror to idle without requiring an exact applied row count.
async fn async_catchup_drain(client: &Client) -> Result<()> {
    let _ = terminate_async_workers_until_idle(client).await;
    loop {
        let applied: i64 = client
            .query_one("SELECT koldstore.wait_for_async_mirror()", &[])
            .await?
            .get(0);
        if applied == 0 {
            break;
        }
    }
    Ok(())
}

async fn disable_async_retained_wal_health_threshold_for_benchmark(
    client: &Client,
    dbname: &str,
) -> Result<()> {
    // With the worker off (or lagging), multi-million-row seeding retains
    // multi-GiB slot WAL until the fence. Disable the health alarm for this
    // controlled lab run; apply remains enabled regardless of this threshold.
    client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.async_mirror_max_retained_bytes = 0"
        ))
        .await
        .context("disable async mirror retained-WAL health threshold for storage comparison")?;
    client
        .batch_execute("SET koldstore.async_mirror_max_retained_bytes = 0")
        .await
        .context("disable async mirror retained-WAL health threshold in benchmark session")?;
    Ok(())
}

async fn disable_async_worker_for_benchmark(client: &Client, dbname: &str) -> Result<bool> {
    // Pin the database setting and the session. The shared-preload launcher
    // connects to `postgres`, so it may still restart appliers; callers also
    // terminate before each catch-up fence.
    client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.internal_async_mirror_worker = off"
        ))
        .await
        .context("pin async mirror worker GUC off for deterministic benchmark phases")?;
    client
        .batch_execute("SET koldstore.internal_async_mirror_worker = off")
        .await
        .context("disable async mirror worker GUC in benchmark session")?;
    disable_async_retained_wal_health_threshold_for_benchmark(client, dbname).await?;
    terminate_async_workers_until_idle(client).await?;
    Ok(true)
}

async fn terminate_async_workers_until_idle(client: &Client) -> Result<()> {
    let started = Instant::now();
    loop {
        let _ = common::terminate_async_worker(client).await?;
        if !common::async_worker_running(client).await? {
            return Ok(());
        }
        if started.elapsed() >= Duration::from_secs(2) {
            common::log_always(
                "storage_cmp: async worker still visible after terminate window; continuing with fence",
            );
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn reset_async_worker_guc(client: &Client, dbname: &str) -> Result<()> {
    client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.internal_async_mirror_worker; \
             ALTER DATABASE \"{dbname}\" RESET koldstore.async_mirror_max_retained_bytes"
        ))
        .await
        .context("reset async mirror GUCs after benchmark")?;
    client
        .batch_execute(
            "RESET koldstore.internal_async_mirror_worker; \
             RESET koldstore.async_mirror_max_retained_bytes",
        )
        .await
        .context("reset async mirror GUCs in benchmark session")?;
    Ok(())
}

async fn assert_async_worker_disabled_for_benchmark(client: &Client) -> Result<()> {
    let disabled: bool = client
        .query_one(
            "SELECT current_setting('koldstore.internal_async_mirror_worker') = 'off'",
            &[],
        )
        .await?
        .get(0);
    anyhow::ensure!(
        disabled,
        "async benchmark worker GUC did not remain disabled"
    );
    // Best-effort: launcher may respawn briefly; catch-up paths terminate again.
    let _ = terminate_async_workers_until_idle(client).await;
    Ok(())
}

async fn checkpoint_before_timing(client: &Client, phase: &str) -> Result<()> {
    let _step = common::log_step_always(format!("storage_cmp: CHECKPOINT before {phase}"));
    client
        .batch_execute("CHECKPOINT")
        .await
        .with_context(|| format!("checkpoint before {phase}"))
}

async fn flush_table(client: &Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass)::text",
            &[&relation],
        )
        .await?;
    let job_id: String = row.get(0);
    let progress = client
        .query_one(
            "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        )
        .await?;
    Ok(progress.get(0))
}

/// Runs `flush_table` while polling cluster RSS so peak memory during Parquet
/// flush is visible (the SQL call itself blocks the session backend).
async fn flush_table_with_metrics(
    client: &Client,
    relation: &str,
    pg_port: u16,
) -> Result<FlushMetrics> {
    let before = common::memory::capture_snapshot(client, pg_port).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let peak_rss = Arc::new(AtomicU64::new(before.rss_bytes));
    let sampler_stop = Arc::clone(&stop);
    let sampler_peak = Arc::clone(&peak_rss);
    let sampler = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while !sampler_stop.load(Ordering::Relaxed) {
            interval.tick().await;
            let sample = cluster_rss_bytes(pg_port).unwrap_or(0);
            let _ = sampler_peak.fetch_max(sample, Ordering::Relaxed);
        }
    });

    let started = Instant::now();
    let rows_flushed = flush_table(client, relation).await?;
    let duration = started.elapsed();

    stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;
    let after = common::memory::capture_snapshot(client, pg_port).await?;
    let peak_rss_bytes = peak_rss
        .load(Ordering::Relaxed)
        .max(before.rss_bytes)
        .max(after.rss_bytes);

    Ok(FlushMetrics {
        rows_flushed,
        duration,
        before_rss_bytes: before.rss_bytes,
        peak_rss_bytes,
        after_rss_bytes: after.rss_bytes,
    })
}

/// Calls `flush_table` until the hot heap is within `hot_limit` (or a call makes
/// no progress). Aggregates duration/rows and keeps the peak RSS across calls.
///
/// Needed when a single job still hits the product wave cap; with the harness
/// `max_rows_per_flush` sized to the cold excess, this usually is one call.
async fn flush_until_hot_limit(
    client: &Client,
    relation: &str,
    pg_port: u16,
    hot_limit: i64,
) -> Result<FlushMetrics> {
    // Bound retries: product jobs also cap at 64 waves; allow a few extra jobs
    // if a prior call drained only a prefix.
    const MAX_FLUSH_JOBS: usize = 8;

    let mut total_rows = 0_i64;
    let mut total_duration = Duration::ZERO;
    let mut before_rss_bytes = 0_u64;
    let mut peak_rss_bytes = 0_u64;
    let mut after_rss_bytes = 0_u64;

    for job in 1..=MAX_FLUSH_JOBS {
        let status = common::describe_table(client, relation).await?;
        if status.hot_rows <= hot_limit {
            if job == 1 {
                anyhow::bail!(
                    "flush requested but hot_rows={} already <= hot_limit={hot_limit}",
                    status.hot_rows
                );
            }
            break;
        }

        let wave = flush_table_with_metrics(client, relation, pg_port).await?;
        if job == 1 {
            before_rss_bytes = wave.before_rss_bytes;
        }
        total_rows = total_rows.saturating_add(wave.rows_flushed);
        total_duration = total_duration.saturating_add(wave.duration);
        peak_rss_bytes = peak_rss_bytes.max(wave.peak_rss_bytes);
        after_rss_bytes = wave.after_rss_bytes;

        common::log_always(format!(
            "storage_cmp: flush job {job}/{MAX_FLUSH_JOBS} rows_flushed={} elapsed={}",
            wave.rows_flushed,
            format_duration(wave.duration),
        ));

        if wave.rows_flushed <= 0 {
            break;
        }
    }

    let status = common::describe_table(client, relation).await?;
    anyhow::ensure!(
        status.hot_rows <= hot_limit,
        "flush did not drain to hot_limit (hot_rows={}, hot_limit={hot_limit}, cold={}, rows_flushed={total_rows})",
        status.hot_rows,
        status.cold_row_count
    );

    Ok(FlushMetrics {
        rows_flushed: total_rows,
        duration: total_duration,
        before_rss_bytes,
        peak_rss_bytes,
        after_rss_bytes,
    })
}

fn cluster_rss_bytes(pg_port: u16) -> Result<u64, String> {
    matched_processes_rss_bytes(&format!(":{pg_port}"))
        .or_else(|_| matched_processes_rss_bytes(&format!("port={pg_port}")))
}

async fn time_vacuum_full(client: &Client, relation: &str) -> Result<Timing> {
    let sql = format!("VACUUM (FULL, ANALYZE) {relation}");
    let started = Instant::now();
    client
        .execute(&sql, &[])
        .await
        .with_context(|| format!("VACUUM failed for {relation}"))?;
    Ok(Timing {
        elapsed: started.elapsed(),
        ops: 1,
        p99_us: None,
    })
}

async fn reindex_relation(client: &Client, relation: &str) -> Result<()> {
    let sql = format!("REINDEX TABLE {relation}");
    client
        .execute(&sql, &[])
        .await
        .with_context(|| format!("REINDEX failed for {relation}"))?;
    Ok(())
}

/// Reads live/dead tuple counters for one user table.
///
/// Call after DML (and `pg_stat_force_next_flush`) so `pg_stat_user_tables`
/// reflects this backend's workload. Pre-flush, both sides should show similar
/// `n_dead_tup` for the same DML sample; the maintenance win shows up in
/// post-flush `VACUUM` time on the smaller managed heap.
async fn heap_stats(client: &Client, relation: &str) -> Result<HeapStats> {
    let row = client
        .query_one(
            r#"
            SELECT
              COALESCE(s.n_live_tup, 0)::bigint,
              COALESCE(s.n_dead_tup, 0)::bigint
            FROM pg_stat_user_tables s
            WHERE s.schemaname = split_part($1, '.', 1)
              AND s.relname = split_part($1, '.', 2)
            "#,
            &[&relation],
        )
        .await
        .with_context(|| format!("pg_stat_user_tables for {relation}"))?;
    Ok(HeapStats {
        n_live_tup: row.get(0),
        n_dead_tup: row.get(1),
    })
}

async fn relation_sizes(
    client: &Client,
    relation: &str,
    managed_relation: Option<&str>,
) -> Result<Sizes> {
    let row = client
        .query_one(
            r#"
            SELECT
              pg_table_size($1::text::regclass)::bigint,
              pg_indexes_size($1::text::regclass)::bigint
            "#,
            &[&relation],
        )
        .await?;
    let table_bytes: i64 = row.get(0);
    let index_bytes: i64 = row.get(1);
    let mut mirror_table_bytes = 0_i64;
    let mut mirror_index_bytes = 0_i64;
    let mut cold_bytes = 0_i64;

    // Managed PostgreSQL footprint must include the latest-state change-log
    // mirror heap and its indexes; omitting `__cl` understates KoldStore cost.
    if let Some(managed) = managed_relation {
        if let Some(mirror) = mirror_relation(managed) {
            let mirror_row = client
                .query_one(
                    r#"
                    SELECT
                      pg_table_size($1::text::regclass)::bigint,
                      pg_indexes_size($1::text::regclass)::bigint
                    "#,
                    &[&mirror],
                )
                .await
                .with_context(|| format!("size mirror {mirror}"))?;
            mirror_table_bytes = mirror_row.get(0);
            mirror_index_bytes = mirror_row.get(1);
        }

        let cold = client
            .query_one(
                r#"
                SELECT COALESCE(sum(byte_size), 0)::bigint
                FROM koldstore.cold_segments
                WHERE table_oid = $1::text::regclass::oid
                  AND status = 'active'
                "#,
                &[&managed],
            )
            .await?;
        cold_bytes = cold.get(0);
    }

    Ok(Sizes {
        table_bytes,
        index_bytes,
        mirror_table_bytes,
        mirror_index_bytes,
        cold_bytes,
    })
}

fn seed_select_sql(start_id: i64, count: i64) -> String {
    let end_id = start_id + count - 1;
    format!(
        r#"
        SELECT
          gs AS id,
          gs % 1024 AS account_id,
          'tenant-' || ((gs % 64)::text) AS tenant_id,
          CASE gs % 5
            WHEN 0 THEN 'click'
            WHEN 1 THEN 'view'
            WHEN 2 THEN 'purchase'
            WHEN 3 THEN 'signup'
            ELSE 'heartbeat'
          END AS event_type,
          CASE gs % 4
            WHEN 0 THEN 'open'
            WHEN 1 THEN 'closed'
            WHEN 2 THEN 'pending'
            ELSE 'archived'
          END AS status,
          (gs % 10)::integer AS priority,
          (gs % 1000)::float8 / 10.0 AS score,
          (gs % 100000)::bigint AS amount_cents,
          (gs % 50)::integer AS quantity,
          (gs % 2 = 0) AS is_active,
          false AS is_deleted,
          'region-' || ((gs % 8)::text) AS region,
          'country-' || ((gs % 32)::text) AS country,
          'city-' || ((gs % 128)::text) AS city,
          CASE gs % 3 WHEN 0 THEN 'web' WHEN 1 THEN 'mobile' ELSE 'api' END AS channel,
          'source-' || ((gs % 16)::text) AS source,
          'campaign-' || ((gs % 20)::text) AS campaign,
          'device-' || ((gs % 12)::text) AS device,
          CASE gs % 3 WHEN 0 THEN 'ios' WHEN 1 THEN 'android' ELSE 'linux' END AS os_name,
          '1.' || ((gs % 9)::text) || '.0' AS app_version,
          'sess-' || lpad((gs % 10000)::text, 8, '0') AS session_id,
          'req-' || lpad(gs::text, 12, '0') AS request_id,
          'trace-' || lpad((gs % 100000)::text, 10, '0') AS trace_id,
          'Mozilla/5.0 row-' || gs::text AS user_agent,
          '10.' || ((gs / 65536) % 256)::text || '.' || ((gs / 256) % 256)::text || '.' || (gs % 256)::text AS ip_address,
          'https://example.com/ref/' || (gs % 100)::text AS referrer,
          '/api/v1/events/' || (gs % 50)::text AS path,
          CASE gs % 4 WHEN 0 THEN 'GET' WHEN 1 THEN 'POST' WHEN 2 THEN 'PUT' ELSE 'DELETE' END AS method,
          (200 + (gs % 20))::integer AS response_code,
          (gs % 500)::integer AS latency_ms,
          (100 + (gs % 9000))::integer AS payload_bytes,
          CASE WHEN gs % 17 = 0 THEN 'E' || (gs % 10)::text ELSE NULL END AS error_code,
          CASE WHEN gs % 17 = 0 THEN 'synthetic error ' || gs::text ELSE NULL END AS error_message,
          'tag-a-' || ((gs % 7)::text) AS tag_a,
          'tag-b-' || ((gs % 11)::text) AS tag_b,
          'tag-c-' || ((gs % 13)::text) AS tag_c,
          'tag-d-' || ((gs % 17)::text) AS tag_d,
          'tag-e-' || ((gs % 19)::text) AS tag_e,
          (gs % 100)::float8 AS metric_1,
          (gs % 200)::float8 / 2.0 AS metric_2,
          (gs % 300)::float8 / 3.0 AS metric_3,
          (gs % 400)::float8 / 4.0 AS metric_4,
          (gs % 500)::float8 / 5.0 AS metric_5,
          (gs % 2 = 0) AS flag_1,
          (gs % 3 = 0) AS flag_2,
          (gs % 5 = 0) AS flag_3,
          (gs % 7 = 0) AS flag_4,
          (gs % 11 = 0) AS flag_5,
          'note-1-' || lpad((gs % 1000)::text, 4, '0') AS note_1,
          'note-2-' || lpad((gs % 2000)::text, 4, '0') AS note_2,
          'note-3-' || lpad((gs % 3000)::text, 4, '0') AS note_3,
          'note-4 payload for row ' || gs::text AS note_4,
          'note-5 longer text body for compression demo row ' || gs::text AS note_5,
          timestamptz '2024-01-01 00:00:00+00' + ((gs % 86400) * interval '1 second') AS created_at,
          timestamptz '2024-01-01 00:00:00+00' + ((gs % 86400) * interval '1 second') AS updated_at
        FROM generate_series({start_id}, {end_id}) AS gs
        "#
    )
}

async fn time_insert(client: &Client, relation: &str, start_id: i64, count: i64) -> Result<Timing> {
    let select = seed_select_sql(start_id, count);
    let started = Instant::now();
    client
        .execute(
            &format!(
                r#"
                INSERT INTO {relation} (
                  id, account_id, tenant_id, event_type, status, priority, score,
                  amount_cents, quantity, is_active, is_deleted, region, country, city,
                  channel, source, campaign, device, os_name, app_version, session_id,
                  request_id, trace_id, user_agent, ip_address, referrer, path, method,
                  response_code, latency_ms, payload_bytes, error_code, error_message,
                  tag_a, tag_b, tag_c, tag_d, tag_e,
                  metric_1, metric_2, metric_3, metric_4, metric_5,
                  flag_1, flag_2, flag_3, flag_4, flag_5,
                  note_1, note_2, note_3, note_4, note_5, created_at, updated_at
                )
                {select}
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("insert into {relation}"))?;
    Ok(Timing {
        elapsed: started.elapsed(),
        ops: count,
        p99_us: None,
    })
}

async fn time_batched_inserts(
    client: &Client,
    relation: &str,
    rows: i64,
    batch_rows: i64,
) -> Result<Timing> {
    // WAL delta makes unfair insert comparisons diagnosable: managed publication
    // should write *more* WAL than plain heap, never less for the same seed SQL.
    let wal_before = current_wal_lsn(client).await?;
    let ckpt_before = checkpoint_counters(client).await?;
    let wal_files_before = wal_file_count(client).await?;
    let mut samples = Vec::new();
    let mut start_id = 1_i64;
    while start_id <= rows {
        let count = batch_rows.min(rows - start_id + 1);
        let batch = time_insert(client, relation, start_id, count).await?;
        samples.push(batch.elapsed);
        start_id += count;
    }
    // p99 is over insert-batch commit latency (not per-row).
    let elapsed: Duration = samples.iter().copied().sum();
    let wal_after = current_wal_lsn(client).await?;
    let wal_bytes = wal_lsn_diff_bytes(client, &wal_before, &wal_after).await?;
    let ckpt_after = checkpoint_counters(client).await?;
    let wal_files_after = wal_file_count(client).await?;
    let first_batch_ms = samples
        .first()
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0);
    let last_batch_ms = samples
        .last()
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0);
    common::log_always(format!(
        "storage_cmp: insert fairness {relation}: batches={} wal_bytes={} ({}) first_batch={:.1}ms last_batch={:.1}ms elapsed={:.3}s ({:.0} rows/s)",
        samples.len(),
        wal_bytes,
        format_bytes(wal_bytes),
        first_batch_ms,
        last_batch_ms,
        elapsed.as_secs_f64(),
        rows as f64 / elapsed.as_secs_f64().max(1e-9),
    ));
    log_insert_batch_curve(&samples);
    common::log_always(format!(
        "storage_cmp: insert checkpoints {relation}: timed=+{} req=+{} write_ms=+{} sync_ms=+{} wal_files={}→{} (delta={:+})",
        ckpt_after.timed.saturating_sub(ckpt_before.timed),
        ckpt_after.req.saturating_sub(ckpt_before.req),
        ckpt_after.write_time_ms.saturating_sub(ckpt_before.write_time_ms),
        ckpt_after.sync_time_ms.saturating_sub(ckpt_before.sync_time_ms),
        wal_files_before,
        wal_files_after,
        wal_files_after as i64 - wal_files_before as i64,
    ));
    Ok(Timing::with_p99(elapsed, rows, &samples))
}

#[derive(Clone, Copy, Debug, Default)]
struct CheckpointCounters {
    timed: i64,
    req: i64,
    write_time_ms: i64,
    sync_time_ms: i64,
}

async fn checkpoint_counters(client: &Client) -> Result<CheckpointCounters> {
    // PG16 keeps checkpoint counters on `pg_stat_bgwriter`. PG17+ moved them to
    // `pg_stat_checkpointer` (renamed columns), so a missing-column error is
    // expected on newer majors — fall through rather than fail the harness.
    if let Ok(Some(row)) = client
        .query_opt(
            r#"
            SELECT
              checkpoints_timed::bigint,
              checkpoints_req::bigint,
              checkpoint_write_time::bigint,
              checkpoint_sync_time::bigint
            FROM pg_stat_bgwriter
            "#,
            &[],
        )
        .await
    {
        return Ok(CheckpointCounters {
            timed: row.get(0),
            req: row.get(1),
            write_time_ms: row.get(2),
            sync_time_ms: row.get(3),
        });
    }
    let row = client
        .query_one(
            r#"
            SELECT
              num_timed::bigint,
              num_requested::bigint,
              write_time::bigint,
              sync_time::bigint
            FROM pg_stat_checkpointer
            "#,
            &[],
        )
        .await
        .context("pg_stat_checkpointer counters")?;
    Ok(CheckpointCounters {
        timed: row.get(0),
        req: row.get(1),
        write_time_ms: row.get(2),
        sync_time_ms: row.get(3),
    })
}

async fn wal_file_count(client: &Client) -> Result<i64> {
    let count: i64 = client
        .query_one(
            r#"
            SELECT count(*)::bigint
            FROM pg_ls_waldir()
            WHERE name ~ '^[0-9A-F]{24}$'
            "#,
            &[],
        )
        .await
        .context("pg_ls_waldir")?
        .get(0);
    Ok(count)
}

/// Logs a compact batch-latency curve so late-run stalls are visible without
/// dumping every sample. Deciles + every 10th batch (1-based).
fn log_insert_batch_curve(samples: &[Duration]) {
    if samples.is_empty() {
        return;
    }
    let mut sorted_ms: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    let n = sorted_ms.len();
    sorted_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pct = |p: f64| -> f64 {
        let idx = ((p * (n as f64 - 1.0)).round() as usize).min(n - 1);
        sorted_ms[idx]
    };
    common::log_always(format!(
        "storage_cmp: insert batch curve percentiles: p0={:.1}ms p25={:.1}ms p50={:.1}ms p75={:.1}ms p90={:.1}ms p99={:.1}ms p100={:.1}ms",
        pct(0.0),
        pct(0.25),
        pct(0.50),
        pct(0.75),
        pct(0.90),
        pct(0.99),
        pct(1.0),
    ));

    let mut parts = Vec::new();
    for (i, sample) in samples.iter().enumerate() {
        let batch_no = i + 1;
        if batch_no == 1 || batch_no == n || batch_no.is_multiple_of(10) {
            parts.push(format!("{batch_no}={:.0}ms", sample.as_secs_f64() * 1000.0));
        }
    }
    common::log_always(format!(
        "storage_cmp: insert batch curve samples: {}",
        parts.join(" ")
    ));
}

async fn current_wal_lsn(client: &Client) -> Result<String> {
    let lsn: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .context("pg_current_wal_lsn")?
        .get(0);
    Ok(lsn)
}

/// Logical slot + publication name used only on the PostgreSQL-only side so
/// timed INSERT sees the same WAL-retention pressure as managed async capture.
const WAL_PIN_SLOT: &str = "koldstore_storage_cmp_wal_pin";
const WAL_PIN_PUBLICATION: &str = "koldstore_storage_cmp_wal_pin_pub";

/// Bring both sides to the same logical-retention starting point before timed INSERT.
///
/// Absolute `wal_files` counts stay in the tens after warm-up because PostgreSQL
/// keeps a recycled segment *pool* near recent write volume (up toward
/// `max_wal_size`) — that is not the same as slot retention. What we equalize
/// is: every logical slot's `restart_lsn` is at the tip (lag ≈ 0), then the
/// timed seed retains newly written WAL the same way on both sides.
async fn prepare_timed_insert_wal_baseline(
    client: &Client,
    side_label: &str,
    drain_async: bool,
) -> Result<()> {
    if drain_async {
        async_catchup_drain(client).await?;
    }
    // Push logical slots to the tip so warm-up segments become recyclable and
    // both sides enter the timed seed with restart_lsn ≈ current_wal_lsn.
    advance_logical_slots_to_tip(client).await?;
    let before = wal_file_count(client).await?;
    for _ in 0..2 {
        client
            .batch_execute("SELECT pg_switch_wal(); CHECKPOINT;")
            .await
            .context("switch+checkpoint while recycling WAL baseline")?;
    }
    // Advance again in case switch_wal created new records behind a slot.
    advance_logical_slots_to_tip(client).await?;
    client
        .batch_execute("CHECKPOINT;")
        .await
        .context("final checkpoint after slot advance")?;
    let after = wal_file_count(client).await?;
    let lag_bytes = logical_slot_lag_bytes(client).await?.unwrap_or(0);
    common::log_always(format!(
        "storage_cmp: timed-insert WAL baseline {side_label}: wal_files={before}→{after}, logical_slot_lag_bytes={lag_bytes}"
    ));
    // restart_lsn often trails the tip by up to one 16MiB segment even after
    // advance; that is still "at tip" for fair seed retention. Fail only on
    // multi-segment warm-up lag that would unequally retain old WAL.
    anyhow::ensure!(
        lag_bytes <= 32 * 1024 * 1024,
        "{side_label} logical slot lag {lag_bytes} bytes before timed insert; want ≤1 WAL segment so both sides retain only seed WAL"
    );
    Ok(())
}

/// Advance every logical slot to `pg_current_wal_lsn()` (no-op when already there).
async fn advance_logical_slots_to_tip(client: &Client) -> Result<()> {
    client
        .batch_execute(
            r#"
            SELECT pg_replication_slot_advance(slot_name, pg_current_wal_lsn())
            FROM pg_replication_slots
            WHERE slot_type = 'logical'
              AND restart_lsn IS NOT NULL
              AND restart_lsn < pg_current_wal_lsn();
            "#,
        )
        .await
        .context("pg_replication_slot_advance to tip")?;
    Ok(())
}

/// Bytes of WAL held behind logical slots (`current - restart_lsn`), if any.
async fn logical_slot_lag_bytes(client: &Client) -> Result<Option<i64>> {
    let row = client
        .query_one(
            r#"
            SELECT COALESCE(
              (
                SELECT max(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))::bigint
                FROM pg_replication_slots
                WHERE slot_type = 'logical'
                  AND restart_lsn IS NOT NULL
              ),
              0
            )
            "#,
            &[],
        )
        .await
        .context("logical slot lag")?;
    let lag: i64 = row.get(0);
    Ok(Some(lag.max(0)))
}

/// Pins WAL during the PostgreSQL-only timed seed.
///
/// Managed async keeps a real slot that does not advance until catch-up, so
/// `max_wal_size` checkpoints cannot recycle segments during seed. Plain PG
/// without a slot recycles aggressively and late insert batches cliff — that
/// is not a product win for KoldStore. This pin mirrors retention only; it is
/// dropped immediately after the timed seed so later VACUUM is unaffected.
///
/// Call [`prepare_timed_insert_wal_baseline`] first so both sides start from
/// the same low `wal_files` count.
async fn pin_wal_retention_for_fair_insert(client: &Client, relation: &str) -> Result<()> {
    unpin_wal_retention_for_fair_insert(client).await?;
    client
        .batch_execute(&format!(
            "CREATE PUBLICATION {WAL_PIN_PUBLICATION} FOR TABLE {relation}"
        ))
        .await
        .with_context(|| format!("create WAL-pin publication for {relation}"))?;
    client
        .execute(
            &format!("SELECT pg_create_logical_replication_slot('{WAL_PIN_SLOT}', 'pgoutput')"),
            &[],
        )
        .await
        .context("create WAL-pin logical slot")?;
    let wal_files = wal_file_count(client).await?;
    common::log_always(format!(
        "storage_cmp: WAL pin enabled for fair pg-only insert (slot={WAL_PIN_SLOT}, publication={WAL_PIN_PUBLICATION}, relation={relation}, wal_files={wal_files})"
    ));
    Ok(())
}

async fn unpin_wal_retention_for_fair_insert(client: &Client) -> Result<()> {
    let dropped_slot: bool = client
        .query_one(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{WAL_PIN_SLOT}')"
            ),
            &[],
        )
        .await?
        .get(0);
    if dropped_slot {
        client
            .execute(
                &format!("SELECT pg_drop_replication_slot('{WAL_PIN_SLOT}')"),
                &[],
            )
            .await
            .context("drop WAL-pin logical slot")?;
    }
    client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {WAL_PIN_PUBLICATION}"))
        .await
        .context("drop WAL-pin publication")?;
    if dropped_slot {
        let wal_files = wal_file_count(client).await.unwrap_or(-1);
        common::log_always(format!(
            "storage_cmp: WAL pin released after pg-only timed seed (wal_files={wal_files})"
        ));
    }
    Ok(())
}

async fn wal_lsn_diff_bytes(client: &Client, before: &str, after: &str) -> Result<i64> {
    let bytes: i64 = client
        .query_one(
            "SELECT pg_wal_lsn_diff($1::text::pg_lsn, $2::text::pg_lsn)::bigint",
            &[&after, &before],
        )
        .await
        .context("pg_wal_lsn_diff")?
        .get(0);
    Ok(bytes.max(0))
}

/// Confirms the timed seed wrote a full contiguous PK range on the user heap.
///
/// Uses point probes (not `count(*)`) so merge-scan materialization cannot OOM
/// at multi-million row scale. Also logs pre-flush heap+index bytes so a
/// "managed insert looked faster" claim can be checked against footprint.
async fn verify_timed_seed(
    client: &Client,
    relation: &str,
    rows: i64,
    side_label: &str,
) -> Result<()> {
    anyhow::ensure!(rows >= 1, "timed seed rows must be >= 1");
    for id in [1_i64, rows / 2, rows] {
        let found: bool = client
            .query_one(
                &format!("SELECT EXISTS(SELECT 1 FROM {relation} WHERE id = $1)"),
                &[&id],
            )
            .await
            .with_context(|| format!("seed probe id={id} on {relation}"))?
            .get(0);
        anyhow::ensure!(
            found,
            "{side_label} timed seed missing id={id} on {relation} (expected contiguous 1..{rows})"
        );
    }
    // Beyond the seeded range must be empty — catches off-by-one / double-seed bugs.
    let overflow: bool = client
        .query_one(
            &format!("SELECT EXISTS(SELECT 1 FROM {relation} WHERE id = $1)"),
            &[&(rows + 1)],
        )
        .await?
        .get(0);
    anyhow::ensure!(
        !overflow,
        "{side_label} timed seed has unexpected id={} on {relation}",
        rows + 1
    );

    let size_row = client
        .query_one(
            r#"
            SELECT
              pg_table_size($1::text::regclass)::bigint,
              pg_indexes_size($1::text::regclass)::bigint
            "#,
            &[&relation],
        )
        .await
        .with_context(|| format!("post-seed sizes for {relation}"))?;
    let table_bytes: i64 = size_row.get(0);
    let index_bytes: i64 = size_row.get(1);
    anyhow::ensure!(
        table_bytes > 0 && index_bytes > 0,
        "{side_label} post-seed sizes must be non-zero (table={table_bytes} index={index_bytes})"
    );
    common::log_always(format!(
        "storage_cmp: seed ok {side_label} {relation}: ids=1..{rows} heap={} indexes={} (pre-flush user relation only)",
        format_bytes(table_bytes),
        format_bytes(index_bytes),
    ));
    Ok(())
}

async fn time_interleaved_inserts(
    client: &Client,
    baseline: &str,
    managed: &str,
    rows: i64,
    batch_rows: i64,
) -> Result<(Timing, Timing)> {
    let mut baseline_samples = Vec::new();
    let mut managed_samples = Vec::new();
    let mut start_id = 1_i64;
    let mut batch_index = 0_u64;

    while start_id <= rows {
        let count = batch_rows.min(rows - start_id + 1);
        if batch_index.is_multiple_of(2) {
            baseline_samples.push(
                time_insert(client, baseline, start_id, count)
                    .await?
                    .elapsed,
            );
            managed_samples.push(time_insert(client, managed, start_id, count).await?.elapsed);
        } else {
            managed_samples.push(time_insert(client, managed, start_id, count).await?.elapsed);
            baseline_samples.push(
                time_insert(client, baseline, start_id, count)
                    .await?
                    .elapsed,
            );
        }
        start_id += count;
        batch_index += 1;
    }

    let baseline_elapsed: Duration = baseline_samples.iter().copied().sum();
    let managed_elapsed: Duration = managed_samples.iter().copied().sum();
    Ok((
        Timing::with_p99(baseline_elapsed, rows, &baseline_samples),
        Timing::with_p99(managed_elapsed, rows, &managed_samples),
    ))
}

async fn time_update(client: &Client, relation: &str, start_id: i64, count: i64) -> Result<Timing> {
    time_batched_updates(
        client,
        relation,
        start_id,
        count,
        DEFAULT_DML_LATENCY_BATCH_ROWS,
    )
    .await
}

async fn time_batched_updates(
    client: &Client,
    relation: &str,
    start_id: i64,
    count: i64,
    batch_rows: i64,
) -> Result<Timing> {
    let batch_rows = batch_rows.clamp(1, count.max(1));
    let mut samples = Vec::new();
    let mut remaining = count;
    let mut cursor = start_id;
    while remaining > 0 {
        let n = batch_rows.min(remaining);
        let end_id = cursor + n - 1;
        let started = Instant::now();
        client
            .execute(
                &format!(
                    r#"
                    UPDATE {relation}
                    SET note_5 = note_5 || ' updated',
                        updated_at = now()
                    WHERE id BETWEEN {cursor} AND {end_id}
                    "#
                ),
                &[],
            )
            .await
            .with_context(|| format!("update {relation}"))?;
        samples.push(started.elapsed());
        cursor = end_id + 1;
        remaining -= n;
    }
    let elapsed: Duration = samples.iter().copied().sum();
    Ok(Timing::with_p99(elapsed, count, &samples))
}

async fn time_delete(
    client: &Client,
    relation: &str,
    start_id: i64,
    end_id: i64,
) -> Result<Timing> {
    let ops = end_id - start_id + 1;
    let started = Instant::now();
    client
        .execute(
            &format!("DELETE FROM {relation} WHERE id BETWEEN {start_id} AND {end_id}"),
            &[],
        )
        .await
        .with_context(|| format!("delete from {relation}"))?;
    Ok(Timing {
        elapsed: started.elapsed(),
        ops,
        p99_us: None,
    })
}

async fn assert_point_row_matches(
    client: &Client,
    baseline: &str,
    managed: &str,
    id: i64,
) -> Result<()> {
    let sql = |relation: &str| {
        format!(
            "SELECT id::text, account_id::text, event_type::text, note_5::text, \
                    amount_cents::text, is_active::text \
             FROM {relation} WHERE id = {id}"
        )
    };
    let baseline_row = client
        .query_one(&sql(baseline), &[])
        .await
        .with_context(|| format!("baseline point row {baseline} id={id}"))?;
    let managed_row = client
        .query_one(&sql(managed), &[])
        .await
        .with_context(|| format!("managed point row {managed} id={id}"))?;
    for (index, column) in [
        "id",
        "account_id",
        "event_type",
        "note_5",
        "amount_cents",
        "is_active",
    ]
    .into_iter()
    .enumerate()
    {
        let baseline_text: String = baseline_row.get(index);
        let managed_text: String = managed_row.get(index);
        anyhow::ensure!(
            baseline_text == managed_text,
            "point lookup mismatch for id={id} column={column}: baseline={baseline_text} managed={managed_text}"
        );
    }
    Ok(())
}

async fn time_point_queries(client: &Client, relation: &str, id: i64) -> Result<Timing> {
    // Use a SQL literal (not `$1`) so timings match EXPLAIN and exercise cold
    // prune + hot equality pushdown the same way applications with Const quals do.
    // Parameterized `$1` is also supported via ParamListInfo resolution.
    let sql = format!("SELECT id, account_id, event_type, note_5 FROM {relation} WHERE id = {id}");
    let total = QUERY_WARMUP_DISCARD + QUERY_LOOPS;
    let mut samples = Vec::with_capacity(QUERY_LOOPS);
    for i in 0..total {
        let started = Instant::now();
        let _ = client
            .query_one(&sql, &[])
            .await
            .with_context(|| format!("point query {relation} id={id}"))?;
        if i >= QUERY_WARMUP_DISCARD {
            samples.push(started.elapsed());
        }
    }
    let elapsed: Duration = samples.iter().copied().sum();
    Ok(Timing::with_p99(elapsed, QUERY_LOOPS as i64, &samples))
}

/// After flush: alternate hot PK / cold PK lookups (50/50 of `QUERY_LOOPS`).
async fn time_mixed_hot_cold_queries(
    client: &Client,
    relation: &str,
    hot_id: i64,
    cold_id: i64,
) -> Result<Timing> {
    let hot_sql =
        format!("SELECT id, account_id, event_type, note_5 FROM {relation} WHERE id = {hot_id}");
    let cold_sql =
        format!("SELECT id, account_id, event_type, note_5 FROM {relation} WHERE id = {cold_id}");
    let total = QUERY_WARMUP_DISCARD + QUERY_LOOPS;
    let mut samples = Vec::with_capacity(QUERY_LOOPS);
    for i in 0..total {
        // Keep 50/50 across the counted window; warmup uses the same cadence.
        let sql = if i % 2 == 0 { &hot_sql } else { &cold_sql };
        let started = Instant::now();
        let _ = client
            .query_one(sql, &[])
            .await
            .with_context(|| format!("mixed hot/cold point query {relation}"))?;
        if i >= QUERY_WARMUP_DISCARD {
            samples.push(started.elapsed());
        }
    }
    let elapsed: Duration = samples.iter().copied().sum();
    Ok(Timing::with_p99(elapsed, QUERY_LOOPS as i64, &samples))
}

#[allow(clippy::too_many_arguments)]
fn print_comparison_table(
    rows: i64,
    hot_limit: i64,
    dml_sample: i64,
    insert_batch_rows: i64,
    max_rows_per_file: i64,
    warmup_rows: i64,
    flush: Option<FlushMetrics>,
    baseline: Option<SideMetrics>,
    managed: Option<SideMetrics>,
    side_label: &str,
) -> Result<()> {
    let report = build_comparison_report(
        rows,
        hot_limit,
        dml_sample,
        insert_batch_rows,
        max_rows_per_file,
        warmup_rows,
        flush,
        baseline,
        managed,
        side_label,
    );
    println!();
    print!("{}", render_comparison_markdown(&report));
    if let Ok(path) = std::env::var("KOLDSTORE_STORAGE_RESULTS_JSON") {
        if !path.is_empty() {
            if let Some(parent) = std::path::Path::new(&path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("create results dir {}", parent.display()))?;
                }
            }
            let json = serde_json::to_string_pretty(&report)
                .context("serialize storage comparison results")?;
            std::fs::write(&path, format!("{json}\n"))
                .with_context(|| format!("write results json {path}"))?;
            common::log_always(format!("storage_cmp: wrote results json {path}"));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ComparisonReport {
    mode: String,
    #[serde(default)]
    generated_at: String,
    /// Full git SHA from `KOLDSTORE_STORAGE_GIT_COMMIT` when the side was measured.
    #[serde(default)]
    git_commit: String,
    /// True when the measuring tree had uncommitted changes (`KOLDSTORE_STORAGE_GIT_DIRTY`).
    #[serde(default)]
    git_dirty: bool,
    #[serde(default)]
    git_note: String,
    rows: i64,
    hot_limit: i64,
    dml_sample: i64,
    insert_batch_rows: i64,
    max_rows_per_file: i64,
    /// Untimed warm-up inserts before the timed seed (`0` = disabled).
    #[serde(default)]
    warmup_rows: i64,
    flushed: i64,
    main: Vec<ComparisonRow>,
    detail: Vec<ComparisonRow>,
    notes: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ComparisonRow {
    metric: String,
    postgres_only: String,
    koldstore: String,
}

#[allow(clippy::too_many_arguments)]
fn build_comparison_report(
    rows: i64,
    hot_limit: i64,
    dml_sample: i64,
    insert_batch_rows: i64,
    max_rows_per_file: i64,
    warmup_rows: i64,
    flush: Option<FlushMetrics>,
    baseline: Option<SideMetrics>,
    managed: Option<SideMetrics>,
    side_label: &str,
) -> ComparisonReport {
    let missing = "—".to_string();
    let local_baseline = baseline.map(|m| m.sizes.pg_total_bytes());
    let local_managed = managed.map(|m| m.sizes.pg_total_bytes());
    let total_managed =
        managed.map(|m| m.sizes.pg_total_bytes().saturating_add(m.sizes.cold_bytes));
    let flush_rows_per_sec = flush
        .filter(|f| !f.duration.is_zero() && f.rows_flushed > 0)
        .map(|f| f.rows_flushed as f64 / f.duration.as_secs_f64())
        .unwrap_or(0.0);
    let flushed_rows = flush.map(|f| f.rows_flushed).unwrap_or(0);

    let pg_ops = |t: Option<Timing>| t.map(format_ops_per_sec).unwrap_or_else(|| missing.clone());
    let mg_ops = |t: Option<Timing>| t.map(format_ops_per_sec).unwrap_or_else(|| missing.clone());
    let pg_speed = |t: Option<Timing>| t.map(format_speed).unwrap_or_else(|| missing.clone());
    let mg_speed = |t: Option<Timing>| t.map(format_speed).unwrap_or_else(|| missing.clone());
    let pg_p99 = |t: Option<Timing>| t.map(format_p99).unwrap_or_else(|| missing.clone());
    let mg_p99 = |t: Option<Timing>| t.map(format_p99).unwrap_or_else(|| missing.clone());
    let pg_dur = |t: Option<Duration>| t.map(format_duration).unwrap_or_else(|| missing.clone());
    let mg_dur = |t: Option<Duration>| t.map(format_duration).unwrap_or_else(|| missing.clone());
    let pg_bytes = |b: Option<i64>| b.map(format_bytes).unwrap_or_else(|| missing.clone());
    let mg_bytes = |b: Option<i64>| b.map(format_bytes).unwrap_or_else(|| missing.clone());

    let main = vec![
        row(
            "foreground insert throughput",
            pg_ops(baseline.map(|m| m.insert)),
            mg_ops(managed.map(|m| m.insert)),
        ),
        row("sustainable insert throughput", "TODO", "TODO"),
        row("sustainable update throughput", "TODO", "TODO"),
        row(
            "insert p99 latency",
            pg_p99(baseline.map(|m| m.insert)),
            mg_p99(managed.map(|m| m.insert)),
        ),
        row(
            "update p99 latency",
            pg_p99(baseline.map(|m| m.update)),
            mg_p99(managed.map(|m| m.update)),
        ),
        row(
            "hot-query p99 latency",
            pg_p99(baseline.map(|m| m.query_hot_only)),
            mg_p99(managed.map(|m| m.query_hot_only)),
        ),
        row(
            "cold-query p99 latency",
            pg_p99(baseline.map(|m| m.query_cold_only)),
            mg_p99(managed.map(|m| m.query_cold_only)),
        ),
        row(
            "hot+cold query throughput",
            pg_ops(baseline.map(|m| m.query_hot_cold)),
            mg_ops(managed.map(|m| m.query_hot_cold)),
        ),
        row(
            "cold-only query throughput",
            pg_ops(baseline.map(|m| m.query_cold_only)),
            mg_ops(managed.map(|m| m.query_cold_only)),
        ),
        row("cold files fetched/query", "—", "TODO"),
        row("cold bytes fetched/query", "—", "TODO"),
        row("peak memory under workload", "TODO", "TODO"),
        row(
            "peak RSS during flush",
            "—",
            flush
                .map(|f| {
                    format!(
                        "{} (before={}, after={})",
                        format_bytes(f.peak_rss_bytes as i64),
                        format_bytes(f.before_rss_bytes as i64),
                        format_bytes(f.after_rss_bytes as i64),
                    )
                })
                .unwrap_or_else(|| missing.clone()),
        ),
        row(
            "flush duration",
            "—",
            flush
                .map(|f| {
                    format!(
                        "{} ({:.0} rows/s)",
                        format_duration(f.duration),
                        flush_rows_per_sec
                    )
                })
                .unwrap_or_else(|| missing.clone()),
        ),
        row("CPU seconds per 1M operations", "TODO", "TODO"),
        row("WAL generated per 1M operations", "TODO", "TODO"),
        row("local bytes written", "TODO", "TODO"),
        row(
            "VACUUM duration",
            pg_dur(baseline.map(|m| m.vacuum.elapsed)),
            mg_dur(managed.map(|m| m.vacuum.elapsed)),
        ),
        row(
            "local PostgreSQL storage",
            pg_bytes(local_baseline),
            mg_bytes(local_managed),
        ),
        row(
            "total hot+cold storage",
            pg_bytes(local_baseline),
            mg_bytes(total_managed),
        ),
        row("peak open file descriptors", "TODO", "TODO"),
        row("combined backup size", "TODO", "TODO"),
        row("full query-ready restore time", "TODO", "TODO"),
        row("mirror backlog after workload", "—", "TODO"),
        row("backlog drain time", "—", "TODO"),
    ];

    let mut detail = vec![
        row(
            "insert speed†",
            pg_speed(baseline.map(|m| m.insert)),
            mg_speed(managed.map(|m| m.insert)),
        ),
        row(
            "update speed†",
            pg_speed(baseline.map(|m| m.update)),
            mg_speed(managed.map(|m| m.update)),
        ),
        row(
            "delete speed†",
            pg_speed(baseline.map(|m| m.delete)),
            mg_speed(managed.map(|m| m.delete)),
        ),
    ];
    if let Some(catchup) = managed.and_then(|m| m.async_catchup) {
        detail.push(row(
            "└ async insert mirror catch-up",
            "—",
            format_speed(catchup.insert),
        ));
        detail.push(row(
            "└ async update mirror catch-up",
            "—",
            format_speed(catchup.update),
        ));
        detail.push(row(
            "└ async delete mirror catch-up",
            "—",
            format_speed(catchup.delete),
        ));
        detail.push(row(
            "└ async restore mirror catch-up",
            "—",
            format_speed(catchup.restore),
        ));
    }
    detail.extend([
        row(
            "query hot only (before flush)",
            pg_speed(baseline.map(|m| m.query_hot_only)),
            mg_speed(managed.map(|m| m.query_hot_only)),
        ),
        row(
            "query with hot+cold (after flush)",
            pg_speed(baseline.map(|m| m.query_hot_cold)),
            mg_speed(managed.map(|m| m.query_hot_cold)),
        ),
        row(
            "query cold only (after flush)",
            pg_speed(baseline.map(|m| m.query_cold_only)),
            mg_speed(managed.map(|m| m.query_cold_only)),
        ),
        row(
            "VACUUM time (after flush)",
            pg_dur(baseline.map(|m| m.vacuum.elapsed)),
            mg_dur(managed.map(|m| m.vacuum.elapsed)),
        ),
        row(
            "dead tuples after workload",
            baseline
                .map(|m| {
                    format!(
                        "{} (live={})",
                        format_count(m.heap_after_workload.n_dead_tup),
                        format_count(m.heap_after_workload.n_live_tup)
                    )
                })
                .unwrap_or_else(|| missing.clone()),
            managed
                .map(|m| {
                    format!(
                        "{} (live={})",
                        format_count(m.heap_after_workload.n_dead_tup),
                        format_count(m.heap_after_workload.n_live_tup)
                    )
                })
                .unwrap_or_else(|| missing.clone()),
        ),
        row(
            "index storage (hot + __cl)",
            pg_bytes(baseline.map(|m| m.sizes.pg_index_bytes())),
            mg_bytes(managed.map(|m| m.sizes.pg_index_bytes())),
        ),
        row(
            "table storage (hot + __cl)",
            pg_bytes(baseline.map(|m| m.sizes.pg_table_bytes())),
            mg_bytes(managed.map(|m| m.sizes.pg_table_bytes())),
        ),
        row(
            "└ cold Parquet",
            "—",
            mg_bytes(managed.map(|m| m.sizes.cold_bytes)),
        ),
        row(
            "└ hot heap only",
            pg_bytes(baseline.map(|m| m.sizes.table_bytes)),
            mg_bytes(managed.map(|m| m.sizes.table_bytes)),
        ),
        row(
            "└ __cl mirror heap",
            "—",
            mg_bytes(managed.map(|m| m.sizes.mirror_table_bytes)),
        ),
        row(
            "└ __cl mirror indexes",
            "—",
            mg_bytes(managed.map(|m| m.sizes.mirror_index_bytes)),
        ),
        row(
            "PostgreSQL heap + indexes (after flush)",
            pg_bytes(local_baseline),
            mg_bytes(local_managed),
        ),
        row("total PG backup size", "TODO", "TODO"),
        row("restore time", "TODO", "TODO"),
    ]);

    let mut notes = Vec::new();
    if let Some(f) = flush {
        notes.push(format!(
            "flush: duration={} rows={} ({:.0} rows/s) peak_rss={} (before={}, after={})",
            format_duration(f.duration),
            f.rows_flushed,
            flush_rows_per_sec,
            format_bytes(f.peak_rss_bytes as i64),
            format_bytes(f.before_rss_bytes as i64),
            format_bytes(f.after_rss_bytes as i64),
        ));
    }
    notes.push(
        "† DML records heap WAL in the foreground; catch-up rows are reported separately after apply.".to_string(),
    );
    if warmup_rows > 0 {
        notes.push(format!(
            "warm-up: untimed insert of {warmup_rows} rows into a throwaway table (same schema/manage mode), then DROP + CHECKPOINT before the timed seed — rejects cold-start insert skew."
        ));
    }
    notes.push(format!(
        "p99: insert = per insert-batch commit; update = per 1k-row update batch; \
         hot/cold query = per PK lookup ({QUERY_LOOPS} timed loops after \
         {QUERY_WARMUP_DISCARD} discarded warm-up lookups)."
    ));
    notes.push(
        "hot-only PK = newest id before flush (full heap both sides). \
         PG cold-id / hot+cold also run on the full heap before VACUUM FULL. \
         Managed cold-only / hot+cold run after flush (Parquet) before hot-heap VACUUM. \
         Timed INSERT seeds an empty table to `rows` on every side — hot_row_limit \
         does not shrink the insert working set."
            .to_string(),
    );
    if baseline.is_none() || managed.is_none() {
        notes.push(
            "isolated side run: only one column filled; merge with --all-sides for RESULTS.md."
                .to_string(),
        );
    }

    ComparisonReport {
        mode: side_label.to_string(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        git_commit: std::env::var("KOLDSTORE_STORAGE_GIT_COMMIT").unwrap_or_default(),
        git_dirty: std::env::var("KOLDSTORE_STORAGE_GIT_DIRTY")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes"))
            .unwrap_or(false),
        git_note: String::new(),
        rows,
        hot_limit,
        dml_sample,
        insert_batch_rows,
        max_rows_per_file,
        warmup_rows,
        flushed: flushed_rows,
        main,
        detail,
        notes,
    }
}

fn row(
    metric: &str,
    postgres_only: impl Into<String>,
    koldstore: impl Into<String>,
) -> ComparisonRow {
    ComparisonRow {
        metric: metric.to_string(),
        postgres_only: postgres_only.into(),
        koldstore: koldstore.into(),
    }
}

fn render_comparison_markdown(report: &ComparisonReport) -> String {
    let mut out = String::new();
    out.push_str("## Main comparison\n\n");
    out.push_str(&format!(
        "schema=tests/storage/schema.sql rows={} hot_row_limit={} \
         dml_sample={} insert_batch_rows={} warmup_rows={} max_rows_per_file={} flushed={} \
         compression=zstd side={}\n\n",
        report.rows,
        report.hot_limit,
        report.dml_sample,
        report.insert_batch_rows,
        report.warmup_rows,
        report.max_rows_per_file,
        report.flushed,
        report.mode
    ));
    out.push_str(&render_two_column_table("Metric", &report.main));
    out.push('\n');
    out.push_str("## Detail (throughput and storage breakdown)\n\n");
    out.push_str(&render_two_column_table("Operation", &report.detail));
    out.push('\n');
    for note in &report.notes {
        out.push_str(note);
        out.push('\n');
    }
    out.push('\n');
    out
}

fn render_two_column_table(label: &str, rows: &[ComparisonRow]) -> String {
    let mut out = String::new();
    out.push_str(&format!("| {label} | PostgreSQL only | PG + KoldStore |\n"));
    out.push_str("| --- | --- | --- |\n");
    for row in rows {
        out.push_str(&format!(
            "| {} | {} | {} |\n",
            row.metric, row.postgres_only, row.koldstore
        ));
    }
    out
}

fn format_ops_per_sec(timing: Timing) -> String {
    format!("{:.0} ops/s", timing.ops_per_sec())
}

fn format_speed(timing: Timing) -> String {
    format!(
        "{:.0} ops/s ({:.0} µs/op)",
        timing.ops_per_sec(),
        timing.per_op_us()
    )
}

fn format_p99(timing: Timing) -> String {
    match timing.p99_us {
        Some(us) if us >= 1000.0 => format!("{:.2} ms", us / 1000.0),
        Some(us) => format!("{us:.0} µs"),
        None => "—".to_string(),
    }
}

fn format_duration(elapsed: Duration) -> String {
    let ms = elapsed.as_secs_f64() * 1000.0;
    if ms >= 1000.0 {
        format!("{:.2} s", elapsed.as_secs_f64())
    } else {
        format!("{ms:.1} ms")
    }
}

fn format_count(value: i64) -> String {
    format!("{value}")
}

fn format_bytes(bytes: i64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let value = bytes as f64;
    if value >= GIB {
        format!("{:.2} GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.2} MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.1} KiB", value / KIB)
    } else {
        format!("{bytes} B")
    }
}
