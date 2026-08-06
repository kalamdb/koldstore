//! Bonded realtime latency for mirror apply and `koldstore.changes_since`.
//!
//! Contract under test (see `docs/architecture/mirror-capture.md`):
//! - Background apply stays live while Parquet upload runs.
//! - Finalize briefly holds the apply/slot lock; these tests keep
//!   `auto_flush => false` and call `flush_table` only when probing that path.
//! - Visibility is measured by polling `changes_since` / `__cl` **without**
//!   calling `wait_for_async_mirror` on the probe path (true background lag).

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use crate::common;
use crate::flush::harness::{barrier_unlock, connect_peer, pause_flush_at};

/// Product SLO: commit → mirror / `changes_since` under open apply lock.
const VISIBILITY_BOUND: Duration = Duration::from_secs(1);

const POLL_SLEEP: Duration = Duration::from_millis(10);

async fn manage_no_auto_flush(db: &common::TestDb, relation: &str) -> Result<()> {
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => NULL,
              min_flush_rows => 1,
              max_rows_per_file => 1000,
              migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[&relation, &db.storage_name],
        )
        .await
        .with_context(|| format!("manage_table auto_flush=false for {relation}"))?;
    common::assert_system_columns_absent(&db.client, relation).await?;
    let table = relation.rsplit('.').next().unwrap_or(relation);
    common::assert_change_log_mirror_exists(&db.client, &format!("koldstore.{table}__cl")).await?;
    common::assert_catalog_has_active_schema(&db.client, relation).await?;
    common::wait_for_async_worker(&db.client).await?;
    Ok(())
}

async fn create_events_table(db: &common::TestDb, name: &str) -> Result<String> {
    let relation = db.relation(name);
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {relation} (
               id bigint PRIMARY KEY,
               body text NOT NULL
             )"
        ))
        .await?;
    manage_no_auto_flush(db, &relation).await?;
    Ok(relation)
}

async fn changes_since_cursor(client: &Client, relation: &str) -> Result<i64> {
    Ok(client
        .query_one(
            "SELECT COALESCE(max(seq), 0)::bigint \
             FROM koldstore.changes_since($1::text::regclass, 0, 100000)",
            &[&relation],
        )
        .await?
        .get(0))
}

/// Polls `changes_since` until `id` appears with `op` after `since_seq`.
///
/// Does **not** call `wait_for_async_mirror` — measures background apply lag.
/// `deadline` is an absolute Instant (typically commit_time + VISIBILITY_BOUND).
async fn wait_changes_since_pk(
    client: &Client,
    relation: &str,
    since_seq: i64,
    id: i64,
    op: i16,
    deadline: Instant,
) -> Result<Duration> {
    let started = Instant::now();
    loop {
        let found: bool = client
            .query_one(
                "SELECT EXISTS (
                   SELECT 1
                   FROM koldstore.changes_since($1::text::regclass, $2::bigint, 1000)
                   WHERE (pk->>'id')::bigint = $3 AND op = $4
                 )",
                &[&relation, &since_seq, &id, &op],
            )
            .await?
            .get(0);
        if found {
            return Ok(started.elapsed());
        }
        if Instant::now() > deadline {
            bail!(
                "changes_since did not show id={id} op={op} for {relation} \
                 within bound (since_seq={since_seq}, elapsed={:?})",
                started.elapsed()
            );
        }
        tokio::time::sleep(POLL_SLEEP).await;
    }
}

/// Polls the hot `__cl` mirror for a PK without fencing.
async fn wait_mirror_pk(
    client: &Client,
    mirror: &str,
    id: i64,
    op: i16,
    deadline: Instant,
) -> Result<Duration> {
    let started = Instant::now();
    loop {
        let found: bool = client
            .query_one(
                &format!("SELECT EXISTS (SELECT 1 FROM {mirror} WHERE id = $1 AND op = $2)"),
                &[&id, &op],
            )
            .await?
            .get(0);
        if found {
            return Ok(started.elapsed());
        }
        if Instant::now() > deadline {
            bail!(
                "mirror {mirror} did not show id={id} op={op} within bound (elapsed={:?})",
                started.elapsed()
            );
        }
        tokio::time::sleep(POLL_SLEEP).await;
    }
}

async fn spawn_noise_writer(
    db: &common::TestDb,
    relation: &str,
    id_base: i64,
) -> Result<(Arc<std::sync::atomic::AtomicBool>, JoinHandle<Result<()>>)> {
    use std::sync::atomic::{AtomicBool, Ordering};

    let stop = Arc::new(AtomicBool::new(false));
    let peer = connect_peer(db).await?;
    let relation = relation.to_string();
    let stop_flag = Arc::clone(&stop);
    let handle = tokio::spawn(async move {
        let mut seq = 0i64;
        while !stop_flag.load(Ordering::Relaxed) {
            seq += 1;
            let id = id_base + (seq % 400);
            peer.execute(
                &format!(
                    "INSERT INTO {relation} (id, body) VALUES ($1, $2)
                     ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body"
                ),
                &[&id, &format!("noise-{seq}")],
            )
            .await?;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(())
    });
    Ok((stop, handle))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changes_since_insert_visible_within_one_second_under_load() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cs_lat_ins").await?;
        let relation = create_events_table(&db, "lat_ins").await?;
        let (stop, noise) = spawn_noise_writer(&db, &relation, 10_000).await?;

        let cursor = changes_since_cursor(&db.client, &relation).await?;
        let probe_id = 1i64;
        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES ($1, 'probe-insert')"),
                &[&probe_id],
            )
            .await?;
        let committed_at = Instant::now();
        let lag = wait_changes_since_pk(
            &db.client,
            &relation,
            cursor,
            probe_id,
            1, // insert
            committed_at + VISIBILITY_BOUND,
        )
        .await?;
        common::log(format!(
            "insert changes_since lag={lag:?} (bound={VISIBILITY_BOUND:?})"
        ));

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        noise.await??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changes_since_update_visible_within_one_second_under_load() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cs_lat_upd").await?;
        let relation = create_events_table(&db, "lat_upd").await?;
        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'v0')"),
                &[],
            )
            .await?;
        // Seed must land before the probe so we measure update apply, not insert.
        common::fence_async_mirror(&db.client).await?;

        let (stop, noise) = spawn_noise_writer(&db, &relation, 20_000).await?;
        let cursor = changes_since_cursor(&db.client, &relation).await?;
        db.client
            .execute(
                &format!("UPDATE {relation} SET body = 'v1' WHERE id = 1"),
                &[],
            )
            .await?;
        let committed_at = Instant::now();
        let lag = wait_changes_since_pk(
            &db.client,
            &relation,
            cursor,
            1,
            2, // update
            committed_at + VISIBILITY_BOUND,
        )
        .await?;
        common::log(format!(
            "update changes_since lag={lag:?} (bound={VISIBILITY_BOUND:?})"
        ));

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        noise.await??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changes_since_delete_visible_within_one_second_under_load() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cs_lat_del").await?;
        let relation = create_events_table(&db, "lat_del").await?;
        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'doomed')"),
                &[],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let (stop, noise) = spawn_noise_writer(&db, &relation, 30_000).await?;
        let cursor = changes_since_cursor(&db.client, &relation).await?;
        db.client
            .execute(&format!("DELETE FROM {relation} WHERE id = 1"), &[])
            .await?;
        let committed_at = Instant::now();
        let lag = wait_changes_since_pk(
            &db.client,
            &relation,
            cursor,
            1,
            3, // delete
            committed_at + VISIBILITY_BOUND,
        )
        .await?;
        common::log(format!(
            "delete changes_since lag={lag:?} (bound={VISIBILITY_BOUND:?})"
        ));

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        noise.await??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mirror_row_visible_within_one_second_without_fence() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cs_lat_mir").await?;
        let relation = create_events_table(&db, "lat_mir").await?;
        let mirror = common::change_log_mirror_relation(&relation);

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (7, 'mirror-probe')"),
                &[],
            )
            .await?;
        let committed_at = Instant::now();
        let lag =
            wait_mirror_pk(&db.client, &mirror, 7, 1, committed_at + VISIBILITY_BOUND).await?;
        common::log(format!(
            "mirror insert lag={lag:?} (bound={VISIBILITY_BOUND:?})"
        ));
        assert!(
            lag <= VISIBILITY_BOUND,
            "mirror lag {lag:?} exceeded {VISIBILITY_BOUND:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_tables_parallel_1k_commits_visible_within_one_second() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cs_lat_4t").await?;
        let mut relations = Vec::with_capacity(4);
        for i in 0..4 {
            relations.push(create_events_table(&db, &format!("lat_t{i}")).await?);
        }

        // Parallel writers: each commits 1000 rows in one transaction.
        let mut writers: Vec<JoinHandle<Result<(String, Instant)>>> = Vec::new();
        for (idx, relation) in relations.iter().enumerate() {
            let peer = connect_peer(&db).await?;
            let relation = relation.clone();
            let id_base = (idx as i64) * 10_000;
            writers.push(tokio::spawn(async move {
                peer.batch_execute("BEGIN").await?;
                peer.execute(
                    &format!(
                        "INSERT INTO {relation} (id, body)
                         SELECT g, 'bulk-' || g::text
                         FROM generate_series($1::bigint, $2::bigint) AS g"
                    ),
                    &[&(id_base + 1), &(id_base + 1000)],
                )
                .await?;
                peer.batch_execute("COMMIT").await?;
                Ok((relation, Instant::now()))
            }));
        }

        let mut commit_times = Vec::new();
        for writer in writers {
            commit_times.push(writer.await??);
        }

        // Each table's full 1k batch must appear in changes_since within 1s of commit.
        for (relation, committed_at) in commit_times {
            let deadline = committed_at + VISIBILITY_BOUND;
            loop {
                let count: i64 = db
                    .client
                    .query_one(
                        "SELECT count(*)::bigint
                         FROM koldstore.changes_since($1::text::regclass, 0, 10000)",
                        &[&relation],
                    )
                    .await?
                    .get(0);
                if count >= 1000 {
                    common::log(format!(
                        "{relation}: 1000 changes_since rows after {:?}",
                        committed_at.elapsed()
                    ));
                    break;
                }
                if Instant::now() > deadline {
                    bail!(
                        "{relation}: only {count}/1000 changes_since rows within \
                         {VISIBILITY_BOUND:?} after commit (elapsed={:?})",
                        committed_at.elapsed()
                    );
                }
                tokio::time::sleep(POLL_SLEEP).await;
            }
        }
    }
    Ok(())
}

/// While Parquet upload holds the failpoint wait (apply lock free), a probe
/// commit must still land in `changes_since` within one second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changes_since_stays_realtime_during_manual_parquet_flush() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cs_lat_flush").await?;
        let relation = create_events_table(&db, "lat_flush").await?;
        db.client
            .batch_execute(&format!(
                "INSERT INTO {relation} (id, body)
                 SELECT g, 'seed-' || g::text FROM generate_series(1, 64) AS g"
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;

        // Inline so the session failpoint is hit in the same backend as flush_table
        // (queue executors would not inherit a session-level SET).
        let dbname: String = db
            .client
            .query_one("SELECT current_database()::text", &[])
            .await?
            .get(0);
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'inline'; \
                 SET koldstore.flush_execution = 'inline'"
            ))
            .await?;

        let (coordinator, flush_handle) =
            pause_flush_at(&db, &relation, "wait:during_parquet_write").await?;

        let probe = connect_peer(&db).await?;
        let cursor = changes_since_cursor(&probe, &relation).await?;
        let probe_id = 9_001i64;
        probe
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES ($1, 'during-parquet')"),
                &[&probe_id],
            )
            .await?;
        let committed_at = Instant::now();
        let lag = wait_changes_since_pk(
            &probe,
            &relation,
            cursor,
            probe_id,
            1,
            committed_at + VISIBILITY_BOUND,
        )
        .await?;
        common::log(format!(
            "during Parquet flush: insert changes_since lag={lag:?}"
        ));

        barrier_unlock(&coordinator).await?;
        let _ = flush_handle.await??;
        let _ = db
            .client
            .batch_execute(&format!(
                "SET koldstore.failpoint = ''; \
                 ALTER DATABASE \"{dbname}\" RESET koldstore.flush_execution; \
                 RESET koldstore.flush_execution"
            ))
            .await;

        // After flush completes, apply is free again — another probe must still
        // meet the same bound (manual flush only; auto_flush stays false).
        let cursor = changes_since_cursor(&db.client, &relation).await?;
        let probe_id = 9_002i64;
        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES ($1, 'after-flush')"),
                &[&probe_id],
            )
            .await?;
        let committed_at = Instant::now();
        let lag = wait_changes_since_pk(
            &db.client,
            &relation,
            cursor,
            probe_id,
            1,
            committed_at + VISIBILITY_BOUND,
        )
        .await?;
        common::log(format!(
            "after manual flush: insert changes_since lag={lag:?}"
        ));
    }
    Ok(())
}
