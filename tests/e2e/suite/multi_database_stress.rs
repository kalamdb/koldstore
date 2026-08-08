//! Multi-database supervisor stress: concurrent WAL apply + flush across DBs.
//!
//! Async capture is database-scoped (slot, applier, apply lock). These fixtures
//! claim several pooled worker databases at once under
//! [`crate::common::acquire_cluster_exclusive`] so the cluster supervisor must
//! keep independent WAL appliers and flush executors correct under load.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use koldstore_memory::evaluate_growth;

use crate::common;
use crate::flush::harness::{connect_peer, join_workers};

const HOT_ROW_LIMIT: i64 = 40;
const DB_COUNT: usize = 3;
const WRITERS_PER_DB: usize = 3;
const FLOOD_MS: u64 = 1_200;
const APPLY_DEADLINE: Duration = Duration::from_secs(45);
const FLUSH_DEADLINE: Duration = Duration::from_secs(45);

fn require_pooled_databases(min: usize) -> Result<()> {
    anyhow::ensure!(
        common::e2e_db_pool_enabled() && common::e2e_pool_size() >= min,
        "multi-database stress needs KOLDSTORE_E2E_DB_POOL=1 and \
         KOLDSTORE_E2E_THREADS>={min} (got pool_enabled={}, threads={})",
        common::e2e_db_pool_enabled(),
        common::e2e_pool_size()
    );
    Ok(())
}

async fn enable_queue_flush(db: &common::TestDb) -> Result<String> {
    let dbname: String = db
        .client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'queue'; \
             SET koldstore.flush_execution = 'queue';"
        ))
        .await
        .context("enable queue flush_execution")?;
    Ok(dbname)
}

async fn reset_queue_flush(db: &common::TestDb, dbname: &str) -> Result<()> {
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_execution; \
             RESET koldstore.flush_execution;"
        ))
        .await
        .ok();
    Ok(())
}

struct ManagedDb {
    db: common::TestDb,
    relation: String,
    mirror: String,
    dbname: String,
}

async fn start_managed_dbs(
    target: common::PgTarget,
    label: &str,
    count: usize,
) -> Result<Vec<ManagedDb>> {
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let db = common::TestDb::start(target.clone(), &format!("{label}_{index}")).await?;
        // Mirror names are `koldstore.<table>__cl` (bare table only). Include the
        // fixture schema so sequential pool-DB reuse cannot share a stale mirror.
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (
                   id bigint PRIMARY KEY,
                   body text NOT NULL
                 )"
            ))
            .await?;
        db.client
            .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => $3::bigint,
                  min_flush_rows => 1,
                  max_rows_per_file => 20,
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name, &HOT_ROW_LIMIT],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;
        common::fence_async_mirror(&db.client).await?;
        let mirror = common::change_log_mirror_relation(&relation);
        let dbname = enable_queue_flush(&db).await?;
        out.push(ManagedDb {
            db,
            relation,
            mirror,
            dbname,
        });
    }
    Ok(out)
}

/// Wait until the change-log mirror has one live row per primary key.
///
/// Writers may UPDATE after INSERT; the mirror is PK-keyed so those land as
/// `op=Update` and must not be counted via insert-only probes.
///
/// `wait_for_async_mirror` returns rows applied in the fence call (not lag).
async fn wait_mirror_rows(
    client: &tokio_postgres::Client,
    mirror: &str,
    expected: i64,
) -> Result<()> {
    let started = Instant::now();
    loop {
        let applied = common::fence_async_mirror(client).await?;
        let actual = common::row_count(client, mirror).await?;
        if actual == expected {
            return Ok(());
        }
        anyhow::ensure!(
            started.elapsed() <= APPLY_DEADLINE,
            "mirror rows for {mirror}: expected {expected}, got {actual} \
             (fence_applied={applied}) within {APPLY_DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_jobs_quiet(client: &tokio_postgres::Client, relation: &str) -> Result<()> {
    let deadline = Instant::now() + FLUSH_DEADLINE;
    loop {
        let active = common::active_job_count(client, relation).await?;
        if active == 0 {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "flush jobs still active for {relation} after {FLUSH_DEADLINE:?} (active={active})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn spawn_writers(
    peers: Vec<tokio_postgres::Client>,
    relation: String,
    next_id: Arc<AtomicI64>,
    stop: Arc<AtomicBool>,
) -> Vec<tokio::task::JoinHandle<Result<()>>> {
    peers
        .into_iter()
        .enumerate()
        .map(|(writer_idx, peer)| {
            let relation = relation.clone();
            let next_id = Arc::clone(&next_id);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let id = next_id.fetch_add(1, Ordering::SeqCst);
                    peer.execute(
                        &format!("INSERT INTO {relation} (id, body) VALUES ($1, $2)"),
                        &[&id, &format!("w{writer_idx}-{id}")],
                    )
                    .await?;
                    if id % 7 == 0 {
                        peer.execute(
                            &format!("UPDATE {relation} SET body = body || '-u' WHERE id = $1"),
                            &[&id],
                        )
                        .await
                        .ok();
                    }
                    if id % 11 == 0 {
                        let _ = peer
                            .query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[])
                            .await?;
                    }
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
        })
        .collect()
}

/// Three databases each keep an independent WAL applier; concurrent inserts must
/// land in every mirror without cross-DB leakage or apply gaps.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_databases_concurrent_wal_apply_stays_complete() -> Result<()> {
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;
    require_pooled_databases(DB_COUNT)?;

    for target in common::scenario_pg_matrix() {
        let managed = start_managed_dbs(target, "mdb_wal", DB_COUNT).await?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut all_handles = Vec::new();
        let mut next_ids = Vec::new();

        for fixture in &managed {
            let next_id = Arc::new(AtomicI64::new(1));
            next_ids.push(Arc::clone(&next_id));
            let mut peers = Vec::with_capacity(WRITERS_PER_DB);
            for _ in 0..WRITERS_PER_DB {
                peers.push(connect_peer(&fixture.db).await?);
            }
            all_handles.extend(spawn_writers(
                peers,
                fixture.relation.clone(),
                next_id,
                Arc::clone(&stop),
            ));
        }

        tokio::time::sleep(Duration::from_millis(FLOOD_MS)).await;
        stop.store(true, Ordering::SeqCst);
        join_workers(all_handles).await?;

        for (idx, fixture) in managed.iter().enumerate() {
            let inserted = next_ids[idx].load(Ordering::SeqCst) - 1;
            anyhow::ensure!(
                inserted >= 40,
                "db{} flood too light ({inserted} rows)",
                idx
            );

            let visible = common::row_count(&fixture.db.client, &fixture.relation).await?;
            anyhow::ensure!(
                visible == inserted,
                "db{} visible rows {visible} != inserted {inserted}",
                idx
            );

            wait_mirror_rows(&fixture.db.client, &fixture.mirror, inserted).await?;

            let foreign: i64 = fixture
                .db
                .client
                .query_one(
                    &format!(
                        "SELECT count(*)::bigint FROM {} WHERE id > $1",
                        fixture.mirror
                    ),
                    &[&inserted],
                )
                .await?
                .get(0);
            anyhow::ensure!(
                foreign == 0,
                "db{} mirror has unexpected ids > {inserted}",
                idx
            );
            let updated = common::mirror_op_count(&fixture.db.client, &fixture.mirror, 2).await?;
            anyhow::ensure!(
                updated > 0,
                "db{} expected some UPDATE mirror rows under mixed writers",
                idx
            );
            anyhow::ensure!(
                common::async_worker_running(&fixture.db.client).await?,
                "db{} WAL applier must stay running",
                idx
            );
        }

        for fixture in &managed {
            reset_queue_flush(&fixture.db, &fixture.dbname).await?;
        }
    }
    Ok(())
}

/// Seed three databases, flood WAL concurrently, then queue-flush all three in
/// parallel once writers have stopped (selection must stay consistent).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_databases_parallel_queue_flush_under_write_load() -> Result<()> {
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;
    require_pooled_databases(DB_COUNT)?;

    for target in common::scenario_pg_matrix() {
        let managed = start_managed_dbs(target, "mdb_flush", DB_COUNT).await?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut writer_handles = Vec::new();
        let mut next_ids = Vec::new();

        for fixture in &managed {
            fixture
                .db
                .client
                .execute(
                    &format!(
                        "INSERT INTO {} (id, body) \
                         SELECT id, 'seed-' || id FROM generate_series(1, $1::bigint) id",
                        fixture.relation
                    ),
                    &[&(HOT_ROW_LIMIT + 60)],
                )
                .await?;

            let next_id = Arc::new(AtomicI64::new(HOT_ROW_LIMIT + 61));
            next_ids.push(Arc::clone(&next_id));
            let mut peers = Vec::with_capacity(WRITERS_PER_DB);
            for _ in 0..WRITERS_PER_DB {
                peers.push(connect_peer(&fixture.db).await?);
            }
            writer_handles.extend(spawn_writers(
                peers,
                fixture.relation.clone(),
                next_id,
                Arc::clone(&stop),
            ));
        }

        tokio::time::sleep(Duration::from_millis(FLOOD_MS)).await;
        stop.store(true, Ordering::SeqCst);
        join_workers(writer_handles).await?;

        for (idx, fixture) in managed.iter().enumerate() {
            let inserted = next_ids[idx].load(Ordering::SeqCst) - 1;
            anyhow::ensure!(
                inserted >= HOT_ROW_LIMIT + 60 + 40,
                "db{} flood too light ({inserted} rows)",
                idx
            );
            let visible = common::row_count(&fixture.db.client, &fixture.relation).await?;
            anyhow::ensure!(
                visible == inserted,
                "db{} visible {visible} != inserted {inserted} before flush",
                idx
            );
            wait_mirror_rows(&fixture.db.client, &fixture.mirror, visible).await?;
        }

        let mut flush_handles = Vec::new();
        for fixture in &managed {
            let peer = connect_peer(&fixture.db).await?;
            let relation = fixture.relation.clone();
            flush_handles.push(tokio::spawn(async move {
                let job_id: String = peer
                    .query_one(
                        "SELECT koldstore.flush_table($1::text::regclass, true)->>'job_id'",
                        &[&relation],
                    )
                    .await?
                    .get(0);
                anyhow::ensure!(!job_id.is_empty(), "flush_table returned empty job_id");
                let flushed = common::wait_for_flush_job_terminal(&peer, &job_id).await?;
                anyhow::ensure!(
                    flushed > 0,
                    "parallel flush must archive rows for {relation}"
                );
                Ok::<_, anyhow::Error>(())
            }));
        }

        for (idx, handle) in flush_handles.into_iter().enumerate() {
            handle
                .await
                .with_context(|| format!("join flush handle {idx}"))??;
        }

        for (idx, fixture) in managed.iter().enumerate() {
            wait_jobs_quiet(&fixture.db.client, &fixture.relation).await?;
            let inserted = next_ids[idx].load(Ordering::SeqCst) - 1;
            let visible = common::row_count(&fixture.db.client, &fixture.relation).await?;
            anyhow::ensure!(
                visible == inserted,
                "db{} visible {visible} != inserted {inserted} after parallel flush",
                idx
            );
            let hot_after = common::hot_row_count(&fixture.db.client, &fixture.relation).await?;
            anyhow::ensure!(
                hot_after <= HOT_ROW_LIMIT,
                "db{} hot {hot_after} exceeds limit {HOT_ROW_LIMIT}",
                idx
            );
            let plan = common::explain(
                &fixture.db.client,
                &format!(
                    "SELECT id, body FROM {} WHERE id IN (1, 2, 3)",
                    fixture.relation
                ),
            )
            .await?;
            common::assert_kold_merge_scan_explain(&plan)?;
            let cold = common::cold_segment_count(&fixture.db.client, &fixture.relation).await?;
            anyhow::ensure!(
                cold >= 1,
                "db{} must publish at least one cold segment",
                idx
            );
        }

        for fixture in &managed {
            reset_queue_flush(&fixture.db, &fixture.dbname).await?;
        }
    }
    Ok(())
}

/// Repeated multi-DB write/flush/merge-scan cycles must not retain unbounded
/// cluster RSS (supervisor + WAL appliers + flush executors across DBs).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_databases_repeated_flush_memory_stays_bounded() -> Result<()> {
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;
    require_pooled_databases(DB_COUNT)?;

    for target in common::scenario_pg_matrix() {
        let managed = start_managed_dbs(target.clone(), "mdb_mem", DB_COUNT).await?;
        let port = target.port;
        let mut samples = Vec::new();
        let warmup = 1usize;
        let measure = 4usize;
        let batch = HOT_ROW_LIMIT + 30;

        for cycle in 0..(warmup + measure) {
            for fixture in &managed {
                let start_id = cycle as i64 * batch + 1;
                let end_id = start_id + batch - 1;
                fixture
                    .db
                    .client
                    .execute(
                        &format!(
                            "INSERT INTO {} (id, body) \
                             SELECT id, 'm-' || id FROM generate_series($1::bigint, $2::bigint) id \
                             ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body",
                            fixture.relation
                        ),
                        &[&start_id, &end_id],
                    )
                    .await?;
            }

            for fixture in &managed {
                common::fence_async_mirror(&fixture.db.client).await?;
                let _ = fixture
                    .db
                    .flush_table_with_force(&fixture.relation, true)
                    .await?;
                wait_jobs_quiet(&fixture.db.client, &fixture.relation).await?;
                let _ = fixture
                    .db
                    .client
                    .query(
                        &format!(
                            "SELECT id, body FROM {} ORDER BY id DESC LIMIT 25",
                            fixture.relation
                        ),
                        &[],
                    )
                    .await?;
                let _ = fixture.db.client.batch_execute("DISCARD PLANS").await;
            }

            if cycle >= warmup {
                samples.push(common::memory::capture_snapshot(&managed[0].db.client, port).await?);
            }
        }

        let budget = common::memory::growth_budget_from_env();
        let evaluation = evaluate_growth(&samples).map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            evaluation.within_budget(budget),
            "multi-database retained memory growth exceeded budget on pg{}: \
             context +{} bytes ({}/cycle), rss +{} bytes ({}/cycle); budget={budget:?}; \
             evaluation={evaluation:?}",
            target.version,
            evaluation.pg_context_growth_bytes,
            evaluation.pg_context_bytes_per_cycle,
            evaluation.rss_growth_bytes,
            evaluation.rss_bytes_per_cycle,
        );

        for fixture in &managed {
            let visible = common::row_count(&fixture.db.client, &fixture.relation).await?;
            anyhow::ensure!(visible > 0, "{} emptied unexpectedly", fixture.relation);
            reset_queue_flush(&fixture.db, &fixture.dbname).await?;
        }
    }
    Ok(())
}
