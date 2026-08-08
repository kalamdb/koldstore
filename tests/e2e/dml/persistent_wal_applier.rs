//! Persistent WAL applier residency and supervisor restart coverage.
//!
//! Assert-enabled Postgres can abort the shared postmaster when sibling tests
//! race logical decoding (`ReorderBuffer` / `txn->ninvalidations == 0`). These
//! fixtures keep a resident WAL applier peeking continuously, so they take
//! [`crate::common::acquire_cluster_exclusive`].

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::common;

const VISIBILITY_BOUND: Duration = Duration::from_secs(1);
const IDLE_PROBE: Duration = Duration::from_secs(2);

async fn wal_applier_pid(client: &tokio_postgres::Client) -> Result<i32> {
    client
        .query_one(
            "SELECT (koldstore.async_mirror_status()->'wal_applier'->>'pid')::integer",
            &[],
        )
        .await?
        .get::<_, Option<i32>>(0)
        .context("persistent WAL applier has no published PID")
}

async fn mirror_insert_count(client: &tokio_postgres::Client, mirror: &str) -> Result<i64> {
    Ok(client
        .query_one(
            &format!("SELECT count(*)::bigint FROM {mirror} WHERE op = 1"),
            &[],
        )
        .await?
        .get(0))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_wal_applier_keeps_pid_while_idle_and_wakes_within_slo() -> Result<()> {
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "persistent_wal_idle").await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);
        let mirror = common::change_log_mirror_relation(&relation);

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;
        common::fence_async_mirror(&db.client).await?;

        let before_pid = wal_applier_pid(&db.client).await?;
        tokio::time::sleep(IDLE_PROBE).await;
        let after_idle_pid = wal_applier_pid(&db.client).await?;
        assert_eq!(
            after_idle_pid, before_pid,
            "WAL applier must remain registered across idle periods instead of exiting"
        );

        let idle_state = db
            .client
            .query_one(
                "SELECT xact_start IS NULL \
                 FROM pg_catalog.pg_stat_activity WHERE pid = $1",
                &[&before_pid],
            )
            .await?;
        assert!(
            idle_state.get::<_, bool>(0),
            "idle WAL applier must not retain an open transaction or MVCC snapshot"
        );

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'wake')"),
                &[],
            )
            .await?;
        let committed_at = Instant::now();
        loop {
            let found: bool = db
                .client
                .query_one(
                    &format!("SELECT EXISTS (SELECT 1 FROM {mirror} WHERE id = 1 AND op = 1)"),
                    &[],
                )
                .await?
                .get(0);
            if found {
                break;
            }
            anyhow::ensure!(
                committed_at.elapsed() < VISIBILITY_BOUND,
                "persistent WAL applier did not publish the committed insert within {VISIBILITY_BOUND:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let after_apply_pid = wal_applier_pid(&db.client).await?;
        assert_eq!(
            after_apply_pid, before_pid,
            "normal commit application must wake the resident process, not replace it"
        );

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&relation],
            )
            .await?;
        let _ = db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_replaces_idle_wal_applier_without_new_dml() -> Result<()> {
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "persistent_wal_restart").await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;
        common::fence_async_mirror(&db.client).await?;

        let original_pid = wal_applier_pid(&db.client).await?;
        anyhow::ensure!(
            common::terminate_async_worker(&db.client).await?,
            "expected to terminate the resident WAL applier"
        );
        common::wait_for_async_worker_auto_restart(&db.client, original_pid).await?;
        let replacement_pid = wal_applier_pid(&db.client).await?;
        assert_ne!(
            replacement_pid, original_pid,
            "supervisor must replace a lost required WAL service even while caught up"
        );

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&relation],
            )
            .await?;
        let _ = db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await?;
    }
    Ok(())
}

/// Resident WAL applier must keep the same PID while concurrent writers and
/// queue flushes flood the database — the production overlap that used to abort
/// assert builds / leave appliers mid-decode under lighter fixtures.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn persistent_wal_applier_survives_concurrent_dml_and_flush_flood() -> Result<()> {
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;

    const WRITERS: usize = 4;
    const FLUSHERS: usize = 3;
    const ROUNDS: i64 = 12;
    const ROWS_PER_ROUND: i64 = 32;
    const HOT_ROW_LIMIT: i64 = 16;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "persistent_wal_flood").await?;
        let dbname: String = db
            .client
            .query_one("SELECT current_database()::text", &[])
            .await?
            .get(0);
        // Queue flush + resident WAL is the production pairing under load.
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.flush_execution = 'queue'; \
                 SET koldstore.flush_execution = 'queue'; \
                 ALTER DATABASE \"{dbname}\" SET koldstore.min_max_rows_per_file = 1; \
                 SET koldstore.min_max_rows_per_file = 1;"
            ))
            .await?;

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
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => $3::bigint,
                  min_flush_rows => 1,
                  max_rows_per_file => 32,
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name, &HOT_ROW_LIMIT],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;
        common::fence_async_mirror(&db.client).await?;

        let before_pid = wal_applier_pid(&db.client).await?;
        let next_id = Arc::new(AtomicI64::new(1));
        let stop = Arc::new(AtomicBool::new(false));
        let pid_ok = Arc::new(AtomicBool::new(true));

        // Watchdog: PID must not churn while load runs.
        let watchdog = common::connect_peer(&db).await?;
        let watchdog_stop = Arc::clone(&stop);
        let watchdog_pid_ok = Arc::clone(&pid_ok);
        let watchdog_handle = tokio::spawn(async move {
            while !watchdog_stop.load(Ordering::Relaxed) {
                match wal_applier_pid(&watchdog).await {
                    Ok(pid) if pid == before_pid => {}
                    Ok(pid) => {
                        eprintln!(
                            "persistent WAL flood: applier PID changed {before_pid} -> {pid}"
                        );
                        watchdog_pid_ok.store(false, Ordering::SeqCst);
                        break;
                    }
                    Err(error) => {
                        eprintln!("persistent WAL flood: status probe failed: {error:#}");
                        watchdog_pid_ok.store(false, Ordering::SeqCst);
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok::<_, anyhow::Error>(())
        });

        let mut writers = Vec::with_capacity(WRITERS);
        for writer_idx in 0..WRITERS {
            let peer = common::connect_peer(&db).await?;
            let relation = relation.clone();
            let next_id = Arc::clone(&next_id);
            let stop = Arc::clone(&stop);
            writers.push(tokio::spawn(async move {
                let mut local_rounds = 0_i64;
                while !stop.load(Ordering::Relaxed) && local_rounds < ROUNDS {
                    let start = next_id.fetch_add(ROWS_PER_ROUND, Ordering::SeqCst);
                    let end = start + ROWS_PER_ROUND - 1;
                    peer.execute(
                        &format!(
                            "INSERT INTO {relation} (id, body) \
                             SELECT gs, 'w{writer_idx}-' || gs \
                             FROM generate_series($1::bigint, $2::bigint) AS gs"
                        ),
                        &[&start, &end],
                    )
                    .await
                    .with_context(|| format!("writer {writer_idx} insert {start}..={end}"))?;
                    // Mix updates so decode sees more than pure inserts.
                    peer.execute(
                        &format!(
                            "UPDATE {relation} SET body = body || '-u' \
                             WHERE id >= $1 AND id <= $2 AND id % 4 = 0"
                        ),
                        &[&start, &end],
                    )
                    .await?;
                    local_rounds += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok::<_, anyhow::Error>(local_rounds)
            }));
        }

        let mut flushers = Vec::with_capacity(FLUSHERS);
        for flusher_idx in 0..FLUSHERS {
            let peer = common::connect_peer(&db).await?;
            let relation = relation.clone();
            let stop = Arc::clone(&stop);
            flushers.push(tokio::spawn(async move {
                let mut completed = 0_i64;
                while !stop.load(Ordering::Relaxed) && completed < ROUNDS {
                    match common::flush_table_job_id(&peer, &relation, true).await {
                        Ok(Some(job_id)) => {
                            let _ = common::wait_for_flush_job_terminal(&peer, &job_id)
                                .await
                                .with_context(|| {
                                    format!("flusher {flusher_idx} wait job {job_id}")
                                })?;
                            completed += 1;
                        }
                        Ok(None) => {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        Err(error) => {
                            // Slot-lock / already-running / enqueue races are
                            // expected under flood (completing job vs insert).
                            let msg = format!("{error:#}");
                            if msg.contains("flush already in progress")
                                || msg.contains("slot lock")
                                || msg.contains("already holds the flush lock")
                                || msg.contains("enqueue flush job returned no active job id")
                            {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                                continue;
                            }
                            return Err(error);
                        }
                    }
                }
                Ok::<_, anyhow::Error>(completed)
            }));
        }

        // Reader proves ordered pages stay complete under flush+apply load.
        let reader = common::connect_peer(&db).await?;
        let reader_relation = relation.clone();
        let reader_stop = Arc::clone(&stop);
        let reader_handle = tokio::spawn(async move {
            let mut checks = 0_i64;
            while !reader_stop.load(Ordering::Relaxed) && checks < ROUNDS * 4 {
                let count: i64 = reader
                    .query_one(
                        &format!("SELECT count(*)::bigint FROM {reader_relation}"),
                        &[],
                    )
                    .await?
                    .get(0);
                if count > 0 {
                    let page: Vec<i64> = reader
                        .query(
                            &format!(
                                "SELECT id FROM {reader_relation} \
                                 WHERE id >= 1 ORDER BY id LIMIT 10"
                            ),
                            &[],
                        )
                        .await?
                        .into_iter()
                        .map(|row| row.get(0))
                        .collect();
                    anyhow::ensure!(
                        !page.is_empty() && page[0] == 1,
                        "ordered LIMIT must start at id=1 under flood, got {page:?}"
                    );
                }
                checks += 1;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Ok::<_, anyhow::Error>(checks)
        });

        tokio::time::sleep(Duration::from_secs(2)).await;
        stop.store(true, Ordering::SeqCst);

        for (idx, handle) in writers.into_iter().enumerate() {
            let rounds = handle
                .await
                .with_context(|| format!("join writer {idx}"))??;
            anyhow::ensure!(rounds > 0, "writer {idx} made no progress");
        }
        let mut flush_rounds = 0_i64;
        for (idx, handle) in flushers.into_iter().enumerate() {
            flush_rounds += handle
                .await
                .with_context(|| format!("join flusher {idx}"))??;
        }
        anyhow::ensure!(
            flush_rounds > 0,
            "at least one force flush must complete during the flood"
        );
        reader_handle.await??;
        watchdog_handle.await??;
        anyhow::ensure!(
            pid_ok.load(Ordering::SeqCst),
            "persistent WAL applier PID must stay stable across DML+flush flood"
        );

        common::fence_async_mirror(&db.client).await?;
        let after_pid = wal_applier_pid(&db.client).await?;
        assert_eq!(
            after_pid, before_pid,
            "WAL applier must still be the same resident process after flood"
        );

        let inserted = next_id.load(Ordering::SeqCst) - 1;
        anyhow::ensure!(inserted > 0, "flood inserted no rows");
        let visible: i64 = db
            .client
            .query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[])
            .await?
            .get(0);
        anyhow::ensure!(
            visible == inserted,
            "visible rows must match inserted ids ({inserted}), got {visible}"
        );

        // Concurrent flushes prune the hot mirror; prove WAL apply + cold merge
        // still expose every live row through changes_since.
        let mut cursor = 0_i64;
        let mut seen = std::collections::BTreeSet::new();
        loop {
            let page = db
                .client
                .query(
                    "SELECT seq, (pk->>'id')::bigint \
                     FROM koldstore.changes_since($1::text::regclass, $2::bigint, 500)",
                    &[&relation, &cursor],
                )
                .await
                .context("changes_since page after flood")?;
            if page.is_empty() {
                break;
            }
            for row in page {
                cursor = row.get(0);
                if let Some(id) = row.get::<_, Option<i64>>(1) {
                    seen.insert(id);
                }
            }
        }
        anyhow::ensure!(
            seen.len() as i64 == inserted,
            "changes_since must cover all {inserted} live ids after flood, got {}",
            seen.len()
        );

        // Flood flushers may still be finishing commits; wait for a quiet queue
        // before the final catch-up flush (avoids enqueue races on active UUID).
        let quiet_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let active = common::active_job_count(&db.client, &relation).await?;
            if active == 0 {
                break;
            }
            anyhow::ensure!(
                Instant::now() < quiet_deadline,
                "flush jobs still active after flood drain budget (active={active})"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let hot_before = common::hot_row_count(&db.client, &relation).await?;
        if hot_before > HOT_ROW_LIMIT {
            let flushed = db.flush_table_with_force(&relation, true).await?;
            anyhow::ensure!(
                flushed > 0,
                "post-flood force flush must archive excess hot rows"
            );
        }
        let hot = common::hot_row_count(&db.client, &relation).await?;
        anyhow::ensure!(
            hot <= HOT_ROW_LIMIT,
            "post-flood hot must be <= {HOT_ROW_LIMIT}, got {hot}"
        );

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&relation],
            )
            .await?;
        let _ = db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await?;
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.flush_execution; \
                 ALTER DATABASE \"{dbname}\" RESET koldstore.min_max_rows_per_file;"
            ))
            .await
            .ok();
    }
    Ok(())
}

/// Kill/restart the resident applier mid flood; supervisor must restore apply
/// without leaving a permanent decode hole.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn persistent_wal_applier_restarts_under_write_flood_without_gaps() -> Result<()> {
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "persistent_wal_restart_flood").await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);
        let mirror = common::change_log_mirror_relation(&relation);

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;
        common::fence_async_mirror(&db.client).await?;

        let original_pid = wal_applier_pid(&db.client).await?;
        let next_id = Arc::new(AtomicI64::new(1));
        let stop = Arc::new(AtomicBool::new(false));

        let mut writers = Vec::new();
        for writer_idx in 0..3 {
            let peer = common::connect_peer(&db).await?;
            let relation = relation.clone();
            let next_id = Arc::clone(&next_id);
            let stop = Arc::clone(&stop);
            writers.push(tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let id = next_id.fetch_add(1, Ordering::SeqCst);
                    peer.execute(
                        &format!("INSERT INTO {relation} (id, body) VALUES ($1, $2)"),
                        &[&id, &format!("w{writer_idx}-{id}")],
                    )
                    .await?;
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Ok::<_, anyhow::Error>(())
            }));
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        anyhow::ensure!(
            common::terminate_async_worker(&db.client).await?,
            "expected to terminate resident WAL applier mid-flood"
        );
        common::wait_for_async_worker_auto_restart(&db.client, original_pid).await?;
        let replacement = wal_applier_pid(&db.client).await?;
        assert_ne!(replacement, original_pid);

        tokio::time::sleep(Duration::from_millis(400)).await;
        stop.store(true, Ordering::SeqCst);
        for (idx, handle) in writers.into_iter().enumerate() {
            handle
                .await
                .with_context(|| format!("join restart-flood writer {idx}"))??;
        }

        common::fence_async_mirror(&db.client).await?;
        let inserted = next_id.load(Ordering::SeqCst) - 1;
        anyhow::ensure!(inserted >= 50, "restart flood too light ({inserted} rows)");
        let visible: i64 = db
            .client
            .query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[])
            .await?
            .get(0);
        anyhow::ensure!(visible == inserted);
        let mirrored = mirror_insert_count(&db.client, &mirror).await?;
        anyhow::ensure!(
            mirrored == inserted,
            "mirror must catch every insert across applier restart ({inserted}), got {mirrored}"
        );

        let missing: i64 = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM generate_series(1, $1::bigint) AS gs \
                     WHERE NOT EXISTS (
                       SELECT 1 FROM {mirror} m WHERE m.id = gs AND m.op = 1
                     )"
                ),
                &[&inserted],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            missing == 0,
            "mirror missing {missing} insert ids after restart flood"
        );

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&relation],
            )
            .await?;
        let _ = db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await?;
    }
    Ok(())
}
