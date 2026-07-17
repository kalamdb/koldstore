//! Shared helpers for concurrent flush E2E coverage.
//!
//! Peer connections, advisory-lock barriers (same key as flush failpoint waits),
//! mixed DML/query workers, and rich-types table fixtures.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use crate::common::{self, ManagedTable, TestDb};

/// Advisory lock key shared with flush `wait:` failpoints (`"KOLD"`).
pub const BARRIER_LOCK_KEY: i64 = 0x4B4F_4C44;

/// Concurrent connections exercising mixed DML + queries during flush.
pub const WORKER_COUNT: usize = 10;

/// Fixed iteration budget for barrier-synchronized workers.
pub const BARRIER_WORKER_LOOPS: usize = 20;

/// Opens a second client against the same pgrx database as `db`.
///
/// # Errors
///
/// Returns an error when the connection fails.
pub async fn connect_peer(db: &TestDb) -> Result<Client> {
    let (client, connection) =
        tokio_postgres::connect(&db.target.connection_string(), tokio_postgres::NoTls)
            .await
            .context("connect peer client")?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("peer connection error: {error}");
        }
    });
    Ok(client)
}

/// Acquires the shared flush/isolation barrier lock (blocks until available).
///
/// # Errors
///
/// Returns an error when PostgreSQL rejects the lock call.
pub async fn barrier_lock(client: &Client) -> Result<()> {
    client
        .execute("SELECT pg_advisory_lock($1)", &[&BARRIER_LOCK_KEY])
        .await?;
    Ok(())
}

/// Releases the shared flush/isolation barrier lock.
///
/// # Errors
///
/// Returns an error when unlock fails.
pub async fn barrier_unlock(client: &Client) -> Result<()> {
    client
        .execute("SELECT pg_advisory_unlock($1)", &[&BARRIER_LOCK_KEY])
        .await?;
    Ok(())
}

/// Polls until a backend is waiting on the failpoint barrier lock.
///
/// # Errors
///
/// Returns an error when the wait query fails or the deadline elapses.
pub async fn wait_until_barrier_waiter(
    coordinator: &Client,
    flush_finished: impl Fn() -> bool,
) -> Result<()> {
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        let waiting = coordinator
            .query_one(
                "SELECT EXISTS (\
                   SELECT 1 FROM pg_catalog.pg_locks \
                   WHERE locktype = 'advisory' \
                     AND classid = 0 \
                     AND objid = $1::bigint \
                     AND granted = false\
                 )",
                &[&BARRIER_LOCK_KEY],
            )
            .await?
            .get::<_, bool>(0);
        if waiting {
            return Ok(());
        }
        if flush_finished() {
            break;
        }
    }
    anyhow::bail!("flush did not reach wait: failpoint barrier")
}

/// Creates and seeds a rich-types table (jsonb/uuid/float8/timestamptz + nullables).
///
/// Uses types known to roundtrip through flush SPI + Parquet. Avoids `text[]` and
/// `numeric` here: manage accepts them, but the flush SPI reader currently fails to
/// coerce those OIDs into `String`.
///
/// # Errors
///
/// Returns an error when DDL or seed SQL fails.
pub async fn create_rich_types_table(
    db: &TestDb,
    table_name: &str,
    rows: i64,
) -> Result<ManagedTable> {
    let relation = db.relation(table_name);
    let title_index = format!("{table_name}_tag_idx");
    db.client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              id bigint PRIMARY KEY,
              payload jsonb NOT NULL,
              tag uuid NOT NULL,
              amount double precision NOT NULL,
              seen_at timestamptz NOT NULL,
              flag boolean NOT NULL,
              note text,
              payload_null jsonb,
              amount_null double precision
            );
            CREATE INDEX {title_index} ON {relation} (tag);
            INSERT INTO {relation} (
              id, payload, tag, amount, seen_at, flag, note, payload_null, amount_null
            )
            SELECT
              gs::bigint,
              jsonb_build_object(
                'row', gs,
                'kind', CASE WHEN gs % 2 = 0 THEN 'even' ELSE 'odd' END,
                'labels', jsonb_build_array('label-' || (gs % 5)::text, 'batch')
              ),
              md5('rich-' || gs::text)::uuid,
              (gs % 1000)::float8 / 10.0,
              timestamptz '2026-01-01 00:00:00+00' + (gs || ' minutes')::interval,
              (gs % 2 = 0),
              CASE WHEN gs % 3 = 0 THEN NULL ELSE 'note-' || gs::text END,
              CASE WHEN gs % 5 = 0 THEN NULL ELSE jsonb_build_object('nullable', gs) END,
              CASE WHEN gs % 4 = 0 THEN NULL ELSE (gs % 50)::float8 / 5.0 END
            FROM generate_series(1, {rows}) AS gs;
            ANALYZE {relation};
            "#
        ))
        .await
        .with_context(|| format!("create rich-types table {relation}"))?;
    Ok(ManagedTable {
        relation,
        table_name: table_name.to_string(),
        title_index,
    })
}

/// Runs mixed INSERT/UPDATE/DELETE/SELECT until `stop` is set or `max_loops` elapse.
///
/// Own-band inserts use `1_000_000 + worker_id * 10_000 + seq` so peers do not collide.
///
/// # Errors
///
/// Returns an error when a statement fails unexpectedly.
pub async fn run_mixed_worker(
    client: Client,
    relation: String,
    worker_id: usize,
    max_loops: Option<usize>,
    stop: Option<Arc<AtomicBool>>,
) -> Result<()> {
    let band_base = 1_000_000i64 + (worker_id as i64) * 10_000;
    let mut seq = 0i64;
    let mut loops = 0usize;
    loop {
        if let Some(limit) = max_loops {
            if loops >= limit {
                break;
            }
        }
        if let Some(flag) = stop.as_ref() {
            if flag.load(Ordering::Relaxed) {
                break;
            }
        }

        seq += 1;
        let id = band_base + seq;
        let account = (worker_id as i64) + 1;
        let title = format!("w{worker_id}-item-{seq:04}");
        let qty = ((worker_id as i32) % 50) + (seq as i32 % 10);

        if let Err(error) = client
            .execute(
                &format!(
                    "INSERT INTO {relation} (id, account_id, title, qty, category) \
                     VALUES ($1, $2, $3, $4, 'worker')"
                ),
                &[&id, &account, &title, &qty],
            )
            .await
        {
            if is_retryable_concurrency_error(&error) {
                continue;
            }
            return Err(error).with_context(|| format!("worker {worker_id} insert id={id}"));
        }

        // Only touch own-band rows so peers do not deadlock on shared seed PKs.
        let update_id = id;
        if let Err(error) = client
            .execute(
                &format!("UPDATE {relation} SET title = title || '-u' WHERE id = $1"),
                &[&update_id],
            )
            .await
        {
            if is_retryable_concurrency_error(&error) {
                continue;
            }
            return Err(error).with_context(|| format!("worker {worker_id} update id={update_id}"));
        }

        if seq > 2 && seq % 3 == 0 {
            let delete_id = band_base + seq - 2;
            if let Err(error) = client
                .execute(
                    &format!("DELETE FROM {relation} WHERE id = $1"),
                    &[&delete_id],
                )
                .await
            {
                if is_retryable_concurrency_error(&error) {
                    continue;
                }
                return Err(error)
                    .with_context(|| format!("worker {worker_id} delete id={delete_id}"));
            }
        }

        // Row may be absent under concurrent deletes; still exercise the merge path.
        // Avoid full-table COUNT(*) here — it widens lock footprint vs flush/prune.
        if let Err(error) = client
            .query(
                &format!("SELECT id, title, qty FROM {relation} WHERE id = $1"),
                &[&update_id],
            )
            .await
        {
            if !is_retryable_concurrency_error(&error) {
                return Err(error)
                    .with_context(|| format!("worker {worker_id} select id={update_id}"));
            }
        }
        if let Err(error) = client
            .query(
                &format!(
                    "SELECT id FROM {relation} WHERE id >= $1 AND id < $2 ORDER BY id LIMIT 5"
                ),
                &[&band_base, &(band_base + 10_000)],
            )
            .await
        {
            if !is_retryable_concurrency_error(&error) {
                return Err(error).with_context(|| format!("worker {worker_id} band select"));
            }
        }

        loops += 1;
        // Safety cap when only a stop flag drives lifetime.
        if stop.is_some() && max_loops.is_none() && loops >= 500 {
            break;
        }
    }
    Ok(())
}

fn is_retryable_concurrency_error(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .map(|db| {
            let code = db.code().code();
            // deadlock_detected / serialization_failure / lock_not_available
            code == "40P01" || code == "40001" || code == "55P03"
        })
        .unwrap_or_else(|| {
            let text = error.to_string();
            text.contains("deadlock detected") || text.contains("could not obtain lock")
        })
}

/// Spawns `WORKER_COUNT` mixed workers that stop when `stop` is set.
pub fn spawn_firehose_workers(
    peers: Vec<Client>,
    relation: String,
    stop: Arc<AtomicBool>,
) -> Vec<JoinHandle<Result<()>>> {
    peers
        .into_iter()
        .enumerate()
        .map(|(worker_id, client)| {
            let relation = relation.clone();
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                run_mixed_worker(client, relation, worker_id, None, Some(stop)).await
            })
        })
        .collect()
}

/// Spawns mixed workers with a fixed loop budget.
pub fn spawn_barrier_workers(
    peers: Vec<Client>,
    relation: String,
    loops: usize,
) -> Vec<JoinHandle<Result<()>>> {
    peers
        .into_iter()
        .enumerate()
        .map(|(worker_id, client)| {
            let relation = relation.clone();
            tokio::spawn(async move {
                run_mixed_worker(client, relation, worker_id, Some(loops), None).await
            })
        })
        .collect()
}

/// Joins worker handles and surfaces the first error.
///
/// # Errors
///
/// Returns an error when any worker panics or returns `Err`.
pub async fn join_workers(handles: Vec<JoinHandle<Result<()>>>) -> Result<()> {
    for (idx, handle) in handles.into_iter().enumerate() {
        handle
            .await
            .with_context(|| format!("join worker {idx}"))?
            .with_context(|| format!("worker {idx} failed"))?;
    }
    Ok(())
}

/// Connects `count` peer clients.
///
/// # Errors
///
/// Returns an error when any peer connection fails.
pub async fn connect_workers(db: &TestDb, count: usize) -> Result<Vec<Client>> {
    let mut peers = Vec::with_capacity(count);
    for _ in 0..count {
        peers.push(connect_peer(db).await?);
    }
    Ok(peers)
}

/// Runs `flush_table` on a peer client and returns `rows_flushed`.
///
/// # Errors
///
/// Returns an error when flush fails, the job is not completed, or lookup fails.
pub async fn flush_table_on(client: &Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass)::text",
            &[&relation],
        )
        .await
        .with_context(|| format!("flush_table {relation}"))?;
    let job_id: String = row.get(0);
    let progress = client
        .query_one(
            r#"
            SELECT rows_flushed, status, coalesce(error_trace, '')
            FROM koldstore.jobs
            WHERE id = $1::text::uuid
            "#,
            &[&job_id],
        )
        .await
        .with_context(|| format!("lookup flush job {job_id}"))?;
    let rows_flushed: i64 = progress.get(0);
    let status: String = progress.get(1);
    let error_trace: String = progress.get(2);
    anyhow::ensure!(
        status == "completed",
        "flush_table {relation} job {job_id} status={status} rows_flushed={rows_flushed}: {error_trace}"
    );
    Ok(rows_flushed)
}

/// Asserts common post-flush invariants for concurrent scenarios.
///
/// # Errors
///
/// Returns an error when catalog or uniqueness checks fail.
pub async fn assert_flush_load_invariants(client: &Client, relation: &str) -> Result<()> {
    common::assert_no_active_jobs(client, relation).await?;
    common::assert_cold_metadata_present(client, relation).await?;
    common::assert_pk_unique(client, relation, &["id"]).await?;
    let count = common::row_count(client, relation).await?;
    anyhow::ensure!(count > 0, "expected rows visible after concurrent flush");
    Ok(())
}
