//! Jobs, table-lock, DROP, and storage-fault scenarios beyond the basic cancel suite.
//!
//! Covers:
//! - DROP while flush holds the table-job lock (later phase than select-only)
//! - DROP while mirror capture is actively absorbing DML
//! - Four concurrent 10k-row flushes
//! - Same-table dual flush fail-fast vs cross-table apply-lock fail-fast
//! - Mid-flush storage directory removal / path corruption

use std::time::Duration;

use anyhow::{Context, Result};
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use crate::common;
use crate::flush::harness::{
    assert_flush_load_invariants, barrier_lock, barrier_unlock, connect_peer, flush_table_on,
    is_retryable_concurrency_error, wait_until_barrier_waiter,
};

/// Matches `TABLE_JOB_LOCK_NAMESPACE` in `job_lock_pg.rs` (single-bigint advisory key).
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
) -> Result<()> {
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => $3,
              min_flush_rows => 1,
              max_rows_per_file => 5000,
              migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[&relation, &db.storage_name, &hot_row_limit],
        )
        .await
        .with_context(|| format!("manage_table {relation}"))?;
    Ok(())
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
        manage_with_hot_limit(&db, &table.relation, 8).await?;
        disable_auto_flush(&db.client, &table.relation).await?;

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = table.relation.clone();
        let flush_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_manifest_publish';")
                .await?;
            let result = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await;
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
        manage_with_hot_limit(&db, &table.relation, 1_000).await?;
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

/// Four tables flush in parallel with ~10k seed rows each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_tables_flush_10k_rows_in_parallel() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_4x10k").await?;
        let mut relations = Vec::new();
        for name in ["p10k_a", "p10k_b", "p10k_c", "p10k_d"] {
            let table = db.create_indexed_items_table(name, 10_000).await?;
            manage_with_hot_limit(&db, &table.relation, 100).await?;
            disable_auto_flush(&db.client, &table.relation).await?;
            relations.push(table.relation);
        }

        let mut handles = Vec::new();
        for relation in &relations {
            let peer = connect_peer(&db).await?;
            let relation = relation.clone();
            handles.push(tokio::spawn(async move {
                flush_table_on(&peer, &relation).await
            }));
        }

        let mut total = 0i64;
        for (idx, handle) in handles.into_iter().enumerate() {
            let rows = handle
                .await
                .with_context(|| format!("join flush handle {idx}"))??;
            assert!(
                rows >= 9_000,
                "table {idx} expected ~9900 flushed (10k-100 hot), got {rows}"
            );
            total = total.saturating_add(rows);
        }
        assert!(
            total >= 9_000 * 4,
            "combined parallel flush rows too low: {total}"
        );

        for relation in &relations {
            assert_flush_load_invariants(&db.client, relation).await?;
            let visible = common::row_count(&db.client, relation).await?;
            assert_eq!(
                visible, 10_000,
                "{relation} must keep all seed rows visible"
            );
        }
    }

    Ok(())
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
        manage_with_hot_limit(&db, &table.relation, 10).await?;
        disable_auto_flush(&db.client, &table.relation).await?;

        let oid = table_oid(&db.client, &table.relation).await?;
        let lock_key = table_job_lock_key(oid);

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let first = connect_peer(&db).await?;
        let first_relation = table.relation.clone();
        let first_handle: JoinHandle<Result<String>> = tokio::spawn(async move {
            first
                .batch_execute("SET koldstore.failpoint = 'wait:after_claim';")
                .await?;
            let row = first
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&first_relation],
                )
                .await
                .context("first flush_table")?;
            first
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            Ok(row.get::<_, String>(0))
        });

        wait_until_barrier_waiter(&coordinator, || first_handle.is_finished()).await?;
        // `after_claim` runs only after `lock_table_job`. Async phase-0 apply can
        // leave a long gap before park; also reject a stale barrier waiter from a
        // prior pooled-DB peer by requiring the table-job lock itself.
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
        let second_relation = table.relation.clone();
        let second_err = second
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass)::text",
                &[&second_relation],
            )
            .await
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

        // After the first releases locks, a retry must succeed.
        let retried = flush_table_on(&db.client, &table.relation).await?;
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

/// A parked flush holds the database apply lock; a second table's flush fails
/// fast instead of waiting for the first to finish.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn parked_flush_fails_fast_other_table_on_apply_lock() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cross_table_apply_lock").await?;
        let table_a = db.create_indexed_items_table("cross_apply_a", 40).await?;
        let table_b = db.create_indexed_items_table("cross_apply_b", 40).await?;
        manage_with_hot_limit(&db, &table_a.relation, 5).await?;
        manage_with_hot_limit(&db, &table_b.relation, 5).await?;
        disable_auto_flush(&db.client, &table_a.relation).await?;
        disable_auto_flush(&db.client, &table_b.relation).await?;

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_a = connect_peer(&db).await?;
        let relation_a = table_a.relation.clone();
        let handle_a: JoinHandle<Result<()>> = tokio::spawn(async move {
            flush_a
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await?;
            let _ = flush_a
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&relation_a],
                )
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
        let err_b = flush_b
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass)::text",
                &[&relation_b],
            )
            .await
            .expect_err("flush B must fail fast while flush A holds the apply lock");
        let detail = err_b
            .as_db_error()
            .map(|e| e.to_string())
            .unwrap_or_else(|| err_b.to_string());
        assert!(
            detail.contains("apply lock") || detail.contains("flush already in progress"),
            "unexpected flush B error: {detail}"
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
        manage_with_hot_limit(&db, &table.relation, 8).await?;
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
            let result = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await;
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
        manage_with_hot_limit(&db, &table.relation, 6).await?;
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
            let _ = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await;
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
        manage_with_hot_limit(&db, &table.relation, 8).await?;
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
            let row = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await
                .context("flush under cancel")?;
            flush_client
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            Ok(row.get::<_, String>(0))
        });

        wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await?;
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
