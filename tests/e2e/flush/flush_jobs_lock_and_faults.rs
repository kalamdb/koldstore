//! Jobs, table-lock, DROP, and storage-fault scenarios beyond the basic cancel suite.
//!
//! Covers:
//! - DROP while flush holds the table-job lock (later phase than select-only)
//! - DROP while mirror capture is actively absorbing DML
//! - Four concurrent 200k-row flushes
//! - Same-table dual flush fail-fast vs cross-table apply-lock fail-fast
//! - Mid-flush storage directory removal / path corruption

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use crate::common;
use crate::flush::harness::{
    assert_flush_load_invariants, barrier_lock, barrier_unlock, connect_peer, flush_table_on,
    flush_table_retrying_entry_locks, is_retryable_concurrency_error, wait_until_barrier_waiter,
};

/// Matches `TABLE_JOB_LOCK_NAMESPACE` in `job_lock.rs` (single-bigint advisory key).
const TABLE_JOB_LOCK_NAMESPACE: i64 = 0x4b54_4a42;

fn table_job_lock_key(table_oid: u32) -> i64 {
    (TABLE_JOB_LOCK_NAMESPACE << 32) | i64::from(table_oid)
}

async fn table_oid(client: &Client, relation: &str) -> Result<u32> {
    let oid = client
        .query_one("SELECT $1::text::regclass::oid::bigint", &[&relation])
        .await
        .with_context(|| format!("resolve oid for {relation}"))?
        .get::<_, i64>(0);
    u32::try_from(oid).context("table oid does not fit u32")
}

async fn disable_auto_flush(client: &Client, relation: &str) -> Result<()> {
    client
        .batch_execute(&format!(
            "SELECT koldstore.set_table_auto_flush('{relation}'::regclass, false)"
        ))
        .await
        .with_context(|| format!("set_table_auto_flush(false) for {relation}"))?;
    Ok(())
}

async fn manage_with_hot_limit(
    db: &common::TestDb,
    relation: &str,
    hot_row_limit: i64,
    max_rows_per_file: i64,
) -> Result<()> {
    // Failpoint fixtures keep max_rows_per_file tiny so policy flush is due for
    // modest row counts (default 1000 would skip undersized excess). Volume /
    // concurrency tests pass a larger value so they do not write thousands of
    // tiny Parquet segments. ALTER DATABASE so flush peers inherit the floor —
    // session SET alone is not enough: execute validates stored
    // max_rows_per_file against the peer GUC and would fail before wait:
    // failpoints.
    let dbname: String = db
        .client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.min_max_rows_per_file = 1; \
             SET koldstore.min_max_rows_per_file = 1;"
        ))
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => $3,
              min_flush_rows => 1,
              max_rows_per_file => $4,
              migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[
                &relation,
                &db.storage_name,
                &hot_row_limit,
                &max_rows_per_file,
            ],
        )
        .await
        .with_context(|| format!("manage_table {relation}"))?;
    Ok(())
}

/// Snapshot of concurrent flush job progress observed from `koldstore.jobs`.
#[derive(Debug, Default)]
struct ParallelFlushTrackerReport {
    max_concurrent_running: usize,
    samples: usize,
    tables_seen_running: usize,
    tables_with_progress: usize,
}

/// Latest flush job interval for one managed table.
#[derive(Debug, Clone)]
struct FlushJobInterval {
    table_oid: u32,
    started_ms: i64,
    finished_ms: i64,
    rows_flushed: i64,
    batches_completed: i32,
}

/// Polls `koldstore.jobs` until `stop` is set, recording overlap + progress.
async fn track_parallel_flush_jobs(
    client: Client,
    table_oids: Vec<u32>,
    stop: Arc<AtomicBool>,
) -> Result<ParallelFlushTrackerReport> {
    let mut report = ParallelFlushTrackerReport::default();
    let mut saw_running: HashMap<u32, bool> = HashMap::new();
    let mut saw_progress: HashMap<u32, bool> = HashMap::new();
    let mut last_line = String::new();
    let oid_list = table_oids
        .iter()
        .map(|oid| i64::from(*oid))
        .collect::<Vec<_>>();

    loop {
        let rows = client
            .query(
                r#"
                SELECT DISTINCT ON (table_oid)
                  table_oid::bigint,
                  status,
                  phase,
                  rows_flushed,
                  progress_current,
                  progress_total,
                  batches_completed
                FROM koldstore.jobs
                WHERE job_type = 'flush'
                  AND table_oid::bigint = ANY ($1::bigint[])
                ORDER BY table_oid, created_at DESC
                "#,
                &[&oid_list],
            )
            .await
            .context("poll concurrent flush jobs")?;

        let mut running = 0usize;
        let mut line = String::from("flush_4x200k jobs:");
        for row in &rows {
            let oid = u32::try_from(row.get::<_, i64>(0)).context("job table_oid")?;
            let status: String = row.get(1);
            let phase: String = row.get(2);
            let rows_flushed: i64 = row.get(3);
            let progress_current: i64 = row.get(4);
            let progress_total: i64 = row.get(5);
            let batches_completed: i32 = row.get(6);
            if status == "running" {
                running = running.saturating_add(1);
                saw_running.insert(oid, true);
            }
            // Phase advances before rows_flushed is non-zero; count either.
            if progress_current > 0
                || rows_flushed > 0
                || batches_completed > 0
                || (status == "running" && phase != "pending")
            {
                saw_progress.insert(oid, true);
            }
            line.push_str(&format!(
                " oid={oid} status={status} phase={phase} rows={rows_flushed} progress={progress_current}/{progress_total} batches={batches_completed};"
            ));
        }
        report.max_concurrent_running = report.max_concurrent_running.max(running);
        report.samples = report.samples.saturating_add(1);
        if !rows.is_empty() && line != last_line {
            eprintln!("{line}");
            last_line = line;
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }
        // Fast poll: encode/finalize windows can be short with large files.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    report.tables_seen_running = saw_running.len();
    report.tables_with_progress = saw_progress.len();
    Ok(report)
}

/// Loads the latest flush job wall-clock interval per table.
async fn latest_flush_job_intervals(
    client: &Client,
    table_oids: &[u32],
) -> Result<Vec<FlushJobInterval>> {
    let oid_list = table_oids
        .iter()
        .map(|oid| i64::from(*oid))
        .collect::<Vec<_>>();
    let rows = client
        .query(
            r#"
            SELECT DISTINCT ON (table_oid)
              table_oid::bigint,
              (extract(epoch FROM started_at) * 1000)::bigint,
              (extract(epoch FROM coalesce(finished_at, clock_timestamp())) * 1000)::bigint,
              rows_flushed,
              batches_completed
            FROM koldstore.jobs
            WHERE job_type = 'flush'
              AND table_oid::bigint = ANY ($1::bigint[])
              AND started_at IS NOT NULL
            ORDER BY table_oid, created_at DESC
            "#,
            &[&oid_list],
        )
        .await
        .context("load flush job intervals")?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(FlushJobInterval {
            table_oid: u32::try_from(row.get::<_, i64>(0)).context("job table_oid")?,
            started_ms: row.get(1),
            finished_ms: row.get(2),
            rows_flushed: row.get(3),
            batches_completed: row.get(4),
        });
    }
    Ok(out)
}

/// Max number of flush intervals that overlap at any single instant.
fn max_interval_overlap(intervals: &[FlushJobInterval]) -> usize {
    #[derive(Clone, Copy)]
    enum Edge {
        Start,
        End,
    }
    let mut edges: Vec<(i64, Edge)> = Vec::new();
    for interval in intervals {
        edges.push((interval.started_ms, Edge::Start));
        edges.push((interval.finished_ms, Edge::End));
    }
    edges.sort_by(|a, b| {
        a.0.cmp(&b.0).then_with(|| match (&a.1, &b.1) {
            // Process starts before ends at the same timestamp so touching
            // intervals still count as concurrent for an instant.
            (Edge::Start, Edge::End) => std::cmp::Ordering::Less,
            (Edge::End, Edge::Start) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        })
    });
    let mut active = 0usize;
    let mut max_active = 0usize;
    for (_, edge) in edges {
        match edge {
            Edge::Start => {
                active = active.saturating_add(1);
                max_active = max_active.max(active);
            }
            Edge::End => {
                active = active.saturating_sub(1);
            }
        }
    }
    max_active
}

async fn count_advisory_waiters(client: &Client, key: i64) -> Result<i64> {
    // Single-bigint advisory keys split into (classid, objid) = (hi32, lo32).
    // Filter by database: pg_locks is cluster-wide and table OIDs collide across
    // pooled worker DBs.
    let classid = ((key as u64) >> 32) as i64;
    let objid = (key as u32) as i64;
    Ok(client
        .query_one(
            r#"
            SELECT count(*)::bigint
            FROM pg_catalog.pg_locks
            WHERE locktype = 'advisory'
              AND database = (SELECT oid FROM pg_catalog.pg_database
                              WHERE datname = current_database())
              AND classid::bigint = $1
              AND objid::bigint = $2
              AND granted = false
            "#,
            &[&classid, &objid],
        )
        .await?
        .get(0))
}

async fn count_advisory_holders(client: &Client, key: i64) -> Result<i64> {
    let classid = ((key as u64) >> 32) as i64;
    let objid = (key as u32) as i64;
    Ok(client
        .query_one(
            r#"
            SELECT count(*)::bigint
            FROM pg_catalog.pg_locks
            WHERE locktype = 'advisory'
              AND database = (SELECT oid FROM pg_catalog.pg_database
                              WHERE datname = current_database())
              AND classid::bigint = $1
              AND objid::bigint = $2
              AND granted = true
            "#,
            &[&classid, &objid],
        )
        .await?
        .get(0))
}

async fn active_jobs_for_oid(client: &Client, table_oid: u32) -> Result<i64> {
    Ok(client
        .query_one(
            r#"
            SELECT count(*)::bigint
            FROM koldstore.jobs
            WHERE table_oid = $1::bigint::oid
              AND status IN ('pending', 'running')
            "#,
            &[&i64::from(table_oid)],
        )
        .await?
        .get(0))
}

async fn wait_until_no_active_jobs_for_oid(client: &Client, table_oid: u32) -> Result<()> {
    for _ in 0..80 {
        if active_jobs_for_oid(client, table_oid).await? == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!(
        "jobs for oid {table_oid} still active: {}",
        active_jobs_for_oid(client, table_oid).await?
    )
}

async fn latest_flush_job(
    client: &Client,
    relation: &str,
) -> Result<(String, String, Option<String>)> {
    let row = client
        .query_one(
            r#"
            SELECT status, phase, error_trace
            FROM koldstore.jobs
            WHERE table_oid = $1::text::regclass::oid
              AND job_type = 'flush'
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
            &[&relation],
        )
        .await
        .with_context(|| format!("latest flush job for {relation}"))?;
    Ok((row.get(0), row.get(1), row.get(2)))
}

fn flush_error_is_expected_after_drop(error: &tokio_postgres::Error) -> bool {
    let detail = error.to_string();
    detail.contains("does not exist")
        || detail.contains("cancel")
        || detail.contains("managed schema")
        || detail.contains("flush")
        || detail.contains("failpoint")
}

/// DROP while flush is parked after publish (holds table-job lock through cleanup).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_table_during_flush_after_manifest_publish() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "drop_flush_post_pub").await?;
        let table = db
            .create_indexed_items_table("drop_flush_post_pub_items", 64)
            .await?;
        manage_with_hot_limit(&db, &table.relation, 8, 8).await?;
        disable_auto_flush(&db.client, &table.relation).await?;

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = table.relation.clone();
        let flush_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_manifest_publish';")
                .await?;
            let result =
                flush_table_retrying_entry_locks(&flush_client, &flush_relation, false).await;
            flush_client
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            match result {
                Ok(_) => Ok(()),
                Err(error) if flush_error_is_expected_after_drop(&error) => Ok(()),
                Err(error) => Err(error.into()),
            }
        });

        wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await?;

        let oid = table_oid(&db.client, &table.relation).await?;
        let lock_key = table_job_lock_key(oid);
        let mut held = false;
        for _ in 0..80 {
            if count_advisory_holders(&db.client, lock_key).await? >= 1 {
                held = true;
                break;
            }
            anyhow::ensure!(
                !flush_handle.is_finished(),
                "flush exited before holding table-job lock at after_manifest_publish"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            held,
            "flush must hold the table-job advisory lock at after_manifest_publish"
        );

        let drop_client = connect_peer(&db).await?;
        let drop_relation = table.relation.clone();
        let drop_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            drop_client
                .batch_execute(&format!("DROP TABLE {drop_relation}"))
                .await
                .context("DROP TABLE during post-publish flush")?;
            Ok(())
        });

        // DROP should block on the table-job lock until flush exits.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            count_advisory_waiters(&db.client, lock_key).await? >= 1 || drop_handle.is_finished(),
            "DROP should wait on the table-job lock (or finish if flush already exited)"
        );

        barrier_unlock(&coordinator).await?;
        flush_handle.await??;
        drop_handle.await??;

        let exists = db
            .client
            .query_one(
                "SELECT to_regclass($1::text) IS NOT NULL",
                &[&table.relation],
            )
            .await?
            .get::<_, bool>(0);
        assert!(!exists, "table must be gone after DROP during flush");
        wait_until_no_active_jobs_for_oid(&db.client, oid).await?;
    }

    Ok(())
}

/// DROP while live DML has been exercising mirror capture (WAL apply).
///
/// Concurrent DROP + DML can deadlock (AccessExclusive vs row locks); stop writers
/// briefly, then DROP, after mirror activity has already run.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn drop_table_while_mirror_capture_is_active() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "drop_while_mirror").await?;
        let table = db
            .create_indexed_items_table("drop_while_mirror_items", 32)
            .await?;
        manage_with_hot_limit(&db, &table.relation, 1_000, 8).await?;
        disable_auto_flush(&db.client, &table.relation).await?;
        let oid = table_oid(&db.client, &table.relation).await?;

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dml_client = connect_peer(&db).await?;
        let dml_relation = table.relation.clone();
        let stop_flag = std::sync::Arc::clone(&stop);
        let dml_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            for seq in 0..500i64 {
                if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let id = 1_000_000 + seq;
                if let Err(error) = dml_client
                    .execute(
                        &format!(
                            "INSERT INTO {dml_relation} (id, account_id, title, qty, category) \
                             VALUES ($1, 1, $2, 1, 'mirror')"
                        ),
                        &[&id, &format!("m-{seq}")],
                    )
                    .await
                {
                    let text = format!("{error:#}");
                    if text.contains("does not exist")
                        || text.contains("managed")
                        || is_retryable_concurrency_error(&error)
                    {
                        return Ok(());
                    }
                    return Err(error).context("mirror DML insert");
                }
            }
            Ok(())
        });

        // Stop writers first, then fence apply, so DROP does not race an
        // in-flight async apply that still holds AccessShare on the heap.
        tokio::time::sleep(Duration::from_millis(80)).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = tokio::time::timeout(Duration::from_secs(5), dml_handle).await;
        let _ = common::fence_async_mirror(&db.client).await;

        let mut dropped = false;
        for _ in 0..8 {
            match db
                .client
                .batch_execute(&format!("DROP TABLE IF EXISTS {}", table.relation))
                .await
            {
                Ok(()) => {
                    dropped = true;
                    break;
                }
                Err(error) if is_retryable_concurrency_error(&error) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error).context("DROP TABLE while mirror capture active"),
            }
        }
        assert!(dropped, "DROP TABLE must succeed after mirror activity");

        let exists = db
            .client
            .query_one(
                "SELECT to_regclass($1::text) IS NOT NULL",
                &[&table.relation],
            )
            .await?
            .get::<_, bool>(0);
        assert!(!exists);
        wait_until_no_active_jobs_for_oid(&db.client, oid).await?;
    }

    Ok(())
}

/// Four tables flush in parallel with ~200k seed rows each.
///
/// Uses a production-scale `max_rows_per_file` so the test measures concurrent
/// flush overlap rather than thousands of tiny Parquet segments. A side task
/// polls `koldstore.jobs` for live progress; concurrency is proven from
/// overlapping `started_at`/`finished_at` intervals on the first wave.
///
/// Finalize serializes on the database slot lock (~10s try-lock budget). Under
/// 200k-row prune that is expected: some first-wave jobs may error with
/// "slot lock" and are retried after the concurrent wave.
#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn four_tables_flush_200k_rows_in_parallel() -> Result<()> {
    common::require_pgrx_server().await?;

    const SEED_ROWS: i64 = 200_000;
    const HOT_ROW_LIMIT: i64 = 100;
    const MAX_ROWS_PER_FILE: i64 = 1_000;
    // Policy flush leaves `hot_row_limit` rows hot.
    const MIN_FLUSHED_PER_TABLE: i64 = SEED_ROWS - HOT_ROW_LIMIT;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_4x200k").await?;
        let mut relations = Vec::new();
        let mut table_oids = Vec::new();
        for name in ["p200k_a", "p200k_b", "p200k_c", "p200k_d"] {
            let table = db.create_indexed_items_table(name, SEED_ROWS).await?;
            // 1000 rows/file ≈ 200 segments/table — multi-batch volume with
            // concurrent overlap, without the tiny-file tax of max_rows_per_file=8.
            manage_with_hot_limit(&db, &table.relation, HOT_ROW_LIMIT, MAX_ROWS_PER_FILE).await?;
            disable_auto_flush(&db.client, &table.relation).await?;
            table_oids.push(table_oid(&db.client, &table.relation).await?);
            relations.push(table.relation);
        }
        // Drain WAL before Nested inline flushes so finalize catch-up stays short.
        common::fence_async_mirror(&db.client).await?;

        // Open peers first so the four flushes can start together.
        let mut peers = Vec::new();
        for _ in &relations {
            peers.push(connect_peer(&db).await?);
        }

        let stop = Arc::new(AtomicBool::new(false));
        let tracker_client = connect_peer(&db).await?;
        let tracker_oids = table_oids.clone();
        let tracker_stop = Arc::clone(&stop);
        let tracker_handle = tokio::spawn(async move {
            track_parallel_flush_jobs(tracker_client, tracker_oids, tracker_stop).await
        });

        let mut handles = Vec::new();
        for (peer, relation) in peers.into_iter().zip(relations.iter()) {
            let relation = relation.clone();
            handles.push(tokio::spawn(async move {
                flush_table_on(&peer, &relation).await
            }));
        }

        let mut need_retry = Vec::new();
        let mut total = 0i64;
        for (idx, handle) in handles.into_iter().enumerate() {
            match handle
                .await
                .with_context(|| format!("join flush handle {idx}"))?
            {
                Ok(rows) => {
                    assert!(
                        rows >= MIN_FLUSHED_PER_TABLE,
                        "table {idx} expected ~{MIN_FLUSHED_PER_TABLE} flushed ({SEED_ROWS}-{HOT_ROW_LIMIT} hot), got {rows}"
                    );
                    total = total.saturating_add(rows);
                }
                Err(error) if flush_failed_on_slot_lock(&error) => {
                    eprintln!(
                        "flush_4x200k table {idx}: first-wave finalize hit slot lock (expected under volume); will retry: {error:#}"
                    );
                    need_retry.push(idx);
                }
                Err(error) => return Err(error),
            }
        }

        stop.store(true, Ordering::Relaxed);
        let tracker = tracker_handle.await.context("join flush job tracker")??;
        let intervals = latest_flush_job_intervals(&db.client, &table_oids).await?;
        let overlap = max_interval_overlap(&intervals);
        for interval in &intervals {
            eprintln!(
                "flush_4x200k interval: oid={} rows={} batches={} start_ms={} finish_ms={} dur_ms={}",
                interval.table_oid,
                interval.rows_flushed,
                interval.batches_completed,
                interval.started_ms,
                interval.finished_ms,
                interval.finished_ms.saturating_sub(interval.started_ms)
            );
        }
        eprintln!(
            "flush_4x200k tracker: max_running={} samples={} tables_running={} tables_progress={} interval_overlap={} retries={}",
            tracker.max_concurrent_running,
            tracker.samples,
            tracker.tables_seen_running,
            tracker.tables_with_progress,
            overlap,
            need_retry.len()
        );
        assert_eq!(
            intervals.len(),
            4,
            "expected one flush job interval per table, got {}",
            intervals.len()
        );
        assert!(
            overlap >= 2,
            "expected overlapping concurrent flush intervals (overlap={overlap}); intervals={intervals:?}"
        );
        // Nested/inline commits job-row progress only when the statement ends, so
        // a peer tracker often never observes status=running. Interval overlap is
        // the concurrency proof; samples just confirm the poller was alive.
        assert!(
            tracker.samples >= 1,
            "jobs tracker must collect at least one sample"
        );
        assert!(
            tracker.max_concurrent_running >= 2
                || tracker.tables_with_progress >= 1
                || overlap >= 2,
            "tracker/intervals must show concurrent flush activity \
             (max_running={}, progress={}, overlap={})",
            tracker.max_concurrent_running,
            tracker.tables_with_progress,
            overlap
        );

        // Finish tables that lost the finalize slot-lock race.
        for idx in need_retry {
            let relation = &relations[idx];
            let rows = retry_flush_after_slot_lock(&db, relation).await?;
            assert!(
                rows >= MIN_FLUSHED_PER_TABLE || common::row_count(&db.client, relation).await? == SEED_ROWS,
                "table {idx} retry expected remaining excess flushed or all rows already visible, got rows_flushed={rows}"
            );
            total = total.saturating_add(rows.max(0));
        }
        assert!(
            total >= MIN_FLUSHED_PER_TABLE,
            "combined parallel+retry flush rows too low: {total}"
        );

        for relation in &relations {
            assert_flush_load_invariants(&db.client, relation).await?;
            let visible = common::row_count(&db.client, relation).await?;
            assert_eq!(
                visible, SEED_ROWS,
                "{relation} must keep all seed rows visible"
            );
            let hot = common::hot_row_count(&db.client, relation).await?;
            assert!(
                hot <= HOT_ROW_LIMIT,
                "{relation} hot rows {hot} exceed hot_row_limit {HOT_ROW_LIMIT}"
            );
        }
    }

    Ok(())
}

fn flush_failed_on_slot_lock(error: &anyhow::Error) -> bool {
    let text = format!("{error:#}").to_ascii_lowercase();
    text.contains("slot lock") || text.contains("apply lock")
}

async fn retry_flush_after_slot_lock(db: &common::TestDb, relation: &str) -> Result<i64> {
    let mut last_error = None;
    for attempt in 1..=8 {
        match db.flush_table_with_force(relation, true).await {
            Ok(rows) => return Ok(rows),
            Err(error) if flush_failed_on_slot_lock(&error) => {
                eprintln!(
                    "flush_4x200k retry {attempt} for {relation} still slot-lock busy; backing off"
                );
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(100 * attempt as u64)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("retry flush exhausted for {relation}")))
}

/// Same-table dual flush: second caller fails fast while the first holds the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn dual_flush_same_table_fails_fast_while_busy() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "dual_flush_lock").await?;
        let table = db
            .create_indexed_items_table("dual_flush_lock_items", 80)
            .await?;
        manage_with_hot_limit(&db, &table.relation, 10, 8).await?;
        disable_auto_flush(&db.client, &table.relation).await?;
        common::fence_async_mirror(&db.client).await?;

        let oid = table_oid(&db.client, &table.relation).await?;
        let lock_key = table_job_lock_key(oid);

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let first = connect_peer(&db).await?;
        let first_relation = table.relation.clone();
        let first_handle: JoinHandle<Result<String>> = tokio::spawn(async move {
            first
                .batch_execute(
                    "SET koldstore.flush_execution = 'inline'; \
                     SET koldstore.failpoint = 'wait:after_claim';",
                )
                .await?;
            let row = flush_table_retrying_entry_locks(&first, &first_relation, false)
                .await
                .context("first flush_table")?;
            first
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            Ok(row.get::<_, String>(0))
        });

        wait_until_barrier_waiter(&coordinator, || first_handle.is_finished()).await?;
        // `after_claim` runs only after session table-job lock + claim.
        let mut held = false;
        for _ in 0..80 {
            if count_advisory_holders(&db.client, lock_key).await? >= 1 {
                held = true;
                break;
            }
            anyhow::ensure!(
                !first_handle.is_finished(),
                "first flush exited before holding table-job lock after claim"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(held, "first flush must hold table-job lock after claim");

        let second = connect_peer(&db).await?;
        second
            .batch_execute("SET koldstore.flush_execution = 'inline'")
            .await?;
        let second_relation = table.relation.clone();
        // Bound the wait: a regression that blocks on the Nested jobs row must
        // not hang the suite for minutes.
        let second_err = tokio::time::timeout(Duration::from_secs(5), async {
            second
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&second_relation],
                )
                .await
        })
        .await
        .context("second flush timed out (must fail fast, not block on jobs row)")?
        .expect_err("second flush must fail fast while first holds the table-job lock");
        let detail = second_err
            .as_db_error()
            .map(|e| e.to_string())
            .unwrap_or_else(|| second_err.to_string());
        assert!(
            detail.contains("flush already in progress"),
            "unexpected second-flush error: {detail}"
        );

        barrier_unlock(&coordinator).await?;
        let first_job = first_handle.await??;
        assert!(!first_job.is_empty(), "first flush must return a job id");

        // After the first releases locks, a retry must acquire the lock. Policy
        // flush may return 0 rows if the parked job already drained excess.
        let retried = db
            .flush_table_with_force(&table.relation, true)
            .await
            .context("retry flush after first released table-job lock")?;
        assert!(retried >= 0);

        common::assert_no_active_jobs(&db.client, &table.relation).await?;
        common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;
        assert_eq!(
            count_advisory_holders(&db.client, lock_key).await?,
            0,
            "table-job lock must be released after both flushes"
        );
    }

    Ok(())
}

/// A parked flush holds the database apply/slot lock during finalize; a second
/// table's flush must not hang forever waiting for it.
///
/// Nested inline skips pre-select apply and only takes the slot lock in
/// finalize (`after_slot_lock`). Flush work errors are recorded on the job row
/// (SQL still returns the job UUID), so B is asserted via terminal status.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn parked_flush_fails_fast_other_table_on_apply_lock() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cross_table_apply_lock").await?;
        let table_a = db.create_indexed_items_table("cross_apply_a", 40).await?;
        let table_b = db.create_indexed_items_table("cross_apply_b", 40).await?;
        manage_with_hot_limit(&db, &table_a.relation, 5, 8).await?;
        manage_with_hot_limit(&db, &table_b.relation, 5, 8).await?;
        disable_auto_flush(&db.client, &table_a.relation).await?;
        disable_auto_flush(&db.client, &table_b.relation).await?;
        common::fence_async_mirror(&db.client).await?;

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_a = connect_peer(&db).await?;
        let relation_a = table_a.relation.clone();
        let handle_a: JoinHandle<Result<()>> = tokio::spawn(async move {
            flush_a
                .batch_execute("SET koldstore.failpoint = 'wait:after_slot_lock';")
                .await?;
            let _ = flush_table_retrying_entry_locks(&flush_a, &relation_a, false)
                .await
                .context("flush A")?;
            flush_a
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            Ok(())
        });

        wait_until_barrier_waiter(&coordinator, || handle_a.is_finished()).await?;

        let flush_b = connect_peer(&db).await?;
        let relation_b = table_b.relation.clone();
        // Product slot-lock budget is ~10s (200 × 50ms). Bound the client wait
        // so a regression that blocks forever fails this suite quickly.
        let started = std::time::Instant::now();
        let job_b: String = tokio::time::timeout(Duration::from_secs(15), async {
            flush_b
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&relation_b],
                )
                .await
        })
        .await
        .context("flush B enqueue timed out (must not block forever on slot lock)")?
        .context("flush B enqueue/run")?
        .get(0);
        let wait_b = tokio::time::timeout(Duration::from_secs(20), async {
            common::wait_for_flush_job_terminal(&db.client, &job_b).await
        })
        .await
        .context("flush B terminal wait exceeded 20s (slot lock deadline regression)")?;
        anyhow::ensure!(
            started.elapsed() < Duration::from_secs(25),
            "cross-table flush contention must resolve within 25s, took {:?}",
            started.elapsed()
        );
        let detail = wait_b
            .as_ref()
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            wait_b.is_err(),
            "flush B must end in job error while A holds the slot/apply lock, got Ok"
        );
        assert!(
            detail.contains("slot lock")
                || detail.contains("apply lock")
                || detail.contains("flush already in progress")
                || detail.contains("deadline"),
            "unexpected flush B terminal error: {detail}"
        );

        barrier_unlock(&coordinator).await?;
        handle_a.await??;

        let rows_b = flush_table_on(&db.client, &table_b.relation).await?;
        assert!(
            rows_b > 0,
            "flush B must succeed after A releases apply lock"
        );

        assert_flush_load_invariants(&db.client, &table_a.relation).await?;
        assert_flush_load_invariants(&db.client, &table_b.relation).await?;
    }

    Ok(())
}

/// Remove the filesystem storage root while flush is mid-flight; hot stays authoritative.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_fails_when_storage_directory_removed_mid_flight() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_rm_storage").await?;
        let table = db
            .create_indexed_items_table("flush_rm_storage_items", 48)
            .await?;
        manage_with_hot_limit(&db, &table.relation, 8, 8).await?;
        disable_auto_flush(&db.client, &table.relation).await?;

        let visible_before = common::row_count(&db.client, &table.relation).await?;
        let storage_root = db.storage_root.clone();

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = table.relation.clone();
        let flush_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await?;
            let result =
                flush_table_retrying_entry_locks(&flush_client, &flush_relation, false).await;
            flush_client
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            match result {
                Ok(row) => {
                    // Job may still complete as error depending on when write fails.
                    let _job_id: String = row.get(0);
                    Ok(())
                }
                Err(_) => Ok(()),
            }
        });

        wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await?;

        // create_dir_all would recreate a missing directory; plant a file so writes fail.
        if storage_root.exists() {
            std::fs::remove_dir_all(&storage_root)
                .with_context(|| format!("remove storage root {}", storage_root.display()))?;
        }
        std::fs::write(&storage_root, b"storage-root-removed")
            .with_context(|| format!("block path {}", storage_root.display()))?;

        barrier_unlock(&coordinator).await?;
        flush_handle.await??;

        let visible_after = common::row_count(&db.client, &table.relation).await?;
        assert_eq!(
            visible_after, visible_before,
            "hot rows must remain visible after storage-root removal mid-flush"
        );
        assert_eq!(
            common::published_manifest_count(&db.client, &table.relation).await?,
            0,
            "no cold manifest may publish after storage root vanishes"
        );

        let (status, _phase, error_trace) = latest_flush_job(&db.client, &table.relation).await?;
        assert_eq!(
            status, "error",
            "flush job must end in error after storage removal, got {status}, err={error_trace:?}"
        );
        assert!(
            error_trace.as_deref().is_some_and(|t| !t.is_empty()),
            "error_trace must explain the storage failure"
        );

        // Restore a real directory so a follow-up flush can recover.
        std::fs::remove_file(&storage_root).ok();
        std::fs::create_dir_all(&storage_root)?;
        let recovered = db.flush_table(&table.relation).await?;
        assert!(
            recovered > 0,
            "flush must recover after storage root is restored"
        );
        assert_flush_load_invariants(&db.client, &table.relation).await?;
    }

    Ok(())
}

/// Replace the storage directory with a plain file mid-flush so object writes fail closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_fails_when_storage_root_replaced_with_file_mid_flight() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_file_block").await?;
        let table = db
            .create_indexed_items_table("flush_file_block_items", 36)
            .await?;
        manage_with_hot_limit(&db, &table.relation, 6, 8).await?;
        disable_auto_flush(&db.client, &table.relation).await?;

        let storage_root = db.storage_root.clone();
        let visible_before = common::row_count(&db.client, &table.relation).await?;

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = table.relation.clone();
        let flush_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await?;
            let _ = flush_table_retrying_entry_locks(&flush_client, &flush_relation, false).await;
            flush_client
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            Ok(())
        });

        wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await?;

        if storage_root.exists() {
            std::fs::remove_dir_all(&storage_root)
                .with_context(|| format!("remove storage root {}", storage_root.display()))?;
        }
        std::fs::write(&storage_root, b"not-a-directory")
            .with_context(|| format!("plant file at {}", storage_root.display()))?;

        barrier_unlock(&coordinator).await?;
        flush_handle.await??;

        assert_eq!(
            common::row_count(&db.client, &table.relation).await?,
            visible_before,
            "hot remains authoritative when storage root is a file"
        );
        assert_eq!(
            common::published_manifest_count(&db.client, &table.relation).await?,
            0
        );
        let (status, _, error_trace) = latest_flush_job(&db.client, &table.relation).await?;
        assert_eq!(
            status, "error",
            "flush must error when storage root is a file, err={error_trace:?}"
        );
    }

    Ok(())
}

/// Cancel + concurrent second flush: cancel request must not leave a stuck running lock owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cancel_running_flush_releases_job_lock_for_retry() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cancel_unlock_retry").await?;
        let table = db
            .create_indexed_items_table("cancel_unlock_retry_items", 60)
            .await?;
        manage_with_hot_limit(&db, &table.relation, 8, 8).await?;
        disable_auto_flush(&db.client, &table.relation).await?;

        let oid = table_oid(&db.client, &table.relation).await?;
        let lock_key = table_job_lock_key(oid);

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = table.relation.clone();
        let flush_handle: JoinHandle<Result<String>> = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await?;
            let row = flush_table_retrying_entry_locks(&flush_client, &flush_relation, false)
                .await
                .context("flush under cancel")?;
            flush_client
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            Ok(row
                .get::<_, Option<String>>(0)
                .filter(|value| !value.is_empty() && value != "null")
                .unwrap_or_default())
        });

        if let Err(error) =
            wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await
        {
            barrier_unlock(&coordinator).await.ok();
            match flush_handle.await {
                Ok(Ok(job_id)) => {
                    return Err(error).context(format!(
                        "flush returned before failpoint wait (job_id={job_id})"
                    ));
                }
                Ok(Err(flush_error)) => {
                    return Err(flush_error)
                        .context(format!("flush exited before failpoint wait ({error})"));
                }
                Err(join_error) => {
                    return Err(error).context(format!("flush task join failed: {join_error}"));
                }
            }
        }
        let cancelled = db
            .client
            .query_one(
                "SELECT koldstore.cancel_table_jobs($1::text::regclass)",
                &[&table.relation],
            )
            .await?
            .get::<_, i64>(0);
        assert!(cancelled >= 1);

        barrier_unlock(&coordinator).await?;
        let _ = flush_handle.await?;

        assert_eq!(
            count_advisory_holders(&db.client, lock_key).await?,
            0,
            "cancel path must release the table-job lock"
        );

        // Retry flush after cancel must be able to acquire the lock and finish.
        let retried = db.flush_table(&table.relation).await?;
        assert!(
            retried > 0 || common::cold_segment_count(&db.client, &table.relation).await? > 0,
            "retry after cancel should flush remaining work or already have cold data"
        );
        common::assert_no_active_jobs(&db.client, &table.relation).await?;
    }

    Ok(())
}
