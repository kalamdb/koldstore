//! Focused diagnostics for the commit -> async mirror -> `changes_since` latency path.
//!
//! These probes intentionally preserve the one-second product SLO. On failure they
//! capture the worker generation state plus WAL/apply cursors so CI can distinguish
//! a missed publication, supervisor/worker startup delay, and apply lag without
//! papering over the regression with a longer timeout or a foreground fence.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use crate::common;
use crate::flush::harness::connect_peer;

const VISIBILITY_BOUND: Duration = Duration::from_secs(1);
const POLL_SLEEP: Duration = Duration::from_millis(10);

struct ProbeStatuses<'a> {
    before_probe: &'a str,
    after_commit: &'a str,
}

async fn create_managed_events_table(db: &common::TestDb, name: &str) -> Result<String> {
    let relation = db.relation(name);
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
              hot_row_limit => NULL::bigint,
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
    common::wait_for_async_worker(&db.client).await?;
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

async fn async_status(client: &Client) -> Result<String> {
    Ok(client
        .query_one("SELECT koldstore.async_mirror_status()::text", &[])
        .await?
        .get(0))
}

async fn timeout_details(
    client: &Client,
    relation: &str,
    id: i64,
    op: i16,
    since_seq: i64,
) -> Result<String> {
    let mirror = common::change_log_mirror_relation(relation);
    let row = client
        .query_one(
            &format!(
                "SELECT \
                   koldstore.async_mirror_status()::text, \
                   EXISTS (SELECT 1 FROM {mirror} WHERE id = $1 AND op = $2), \
                   COALESCE((SELECT max(seq)::bigint FROM {mirror}), 0), \
                   (SELECT count(*)::bigint \
                      FROM koldstore.changes_since($3::text::regclass, $4::bigint, 1000))"
            ),
            &[&id, &op, &relation, &since_seq],
        )
        .await?;
    let status: String = row.get(0);
    let mirror_found: bool = row.get(1);
    let mirror_max_seq: i64 = row.get(2);
    let changes_since_rows: i64 = row.get(3);
    Ok(format!(
        "status={status}; mirror_found={mirror_found}; mirror_max_seq={mirror_max_seq}; changes_since_rows={changes_since_rows}"
    ))
}

async fn wait_changes_since_pk(
    client: &Client,
    relation: &str,
    since_seq: i64,
    id: i64,
    op: i16,
    deadline: Instant,
    statuses: ProbeStatuses<'_>,
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
            let timeout = timeout_details(client, relation, id, op, since_seq).await?;
            bail!(
                "changes_since latency diagnostic timed out for id={id} op={op} relation={relation} \
                 since_seq={since_seq} elapsed={:?}; before_probe={}; after_commit={}; timeout={timeout}",
                started.elapsed(),
                statuses.before_probe,
                statuses.after_commit
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
async fn diagnose_insert_visibility_under_managed_commit_load() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cs_diag_ins").await?;
        let relation = create_managed_events_table(&db, "diag_ins").await?;
        let (stop, noise) = spawn_noise_writer(&db, &relation, 110_000).await?;

        let cursor = changes_since_cursor(&db.client, &relation).await?;
        let before_probe = async_status(&db.client).await?;
        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'probe-insert')"),
                &[],
            )
            .await?;
        let committed_at = Instant::now();
        let after_commit = async_status(&db.client).await?;
        let outcome = wait_changes_since_pk(
            &db.client,
            &relation,
            cursor,
            1,
            1,
            committed_at + VISIBILITY_BOUND,
            ProbeStatuses {
                before_probe: &before_probe,
                after_commit: &after_commit,
            },
        )
        .await;

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        noise.await??;
        let lag = outcome?;
        common::log(format!(
            "diagnostic insert lag={lag:?}; before_probe={before_probe}; after_commit={after_commit}; final={}",
            async_status(&db.client).await?
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diagnose_delete_visibility_under_managed_commit_load() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cs_diag_del").await?;
        let relation = create_managed_events_table(&db, "diag_del").await?;
        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'doomed')"),
                &[],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let (stop, noise) = spawn_noise_writer(&db, &relation, 130_000).await?;
        let cursor = changes_since_cursor(&db.client, &relation).await?;
        let before_probe = async_status(&db.client).await?;
        db.client
            .execute(&format!("DELETE FROM {relation} WHERE id = 1"), &[])
            .await?;
        let committed_at = Instant::now();
        let after_commit = async_status(&db.client).await?;
        let outcome = wait_changes_since_pk(
            &db.client,
            &relation,
            cursor,
            1,
            3,
            committed_at + VISIBILITY_BOUND,
            ProbeStatuses {
                before_probe: &before_probe,
                after_commit: &after_commit,
            },
        )
        .await;

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        noise.await??;
        let lag = outcome?;
        common::log(format!(
            "diagnostic delete lag={lag:?}; before_probe={before_probe}; after_commit={after_commit}; final={}",
            async_status(&db.client).await?
        ));
    }
    Ok(())
}
