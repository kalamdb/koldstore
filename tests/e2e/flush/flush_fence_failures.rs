//! Async flush fence failure and multi-table WAL stress coverage.

use anyhow::{Context, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::common;
use crate::flush::harness::{
    barrier_lock, barrier_unlock, connect_peer, wait_until_barrier_waiter,
};

/// Stuck writer holding ROW EXCLUSIVE must block the prune fence; hot stays
/// authoritative until the writer finishes, then flush can complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_flush_fence_waits_on_writer_without_pruning_early() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async fence writer-block in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "fence_lock_to").await?;
        let table = db
            .create_indexed_items_table("fence_lock_items", 24)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;

        let blocker = connect_peer(&db).await?;
        blocker.batch_execute("BEGIN").await?;
        blocker
            .execute(
                &format!(
                    "UPDATE {} SET title = title || '-hold' WHERE id = 1",
                    table.relation
                ),
                &[],
            )
            .await
            .context("open writer txn holding row lock")?;

        let flush_client = connect_peer(&db).await?;
        let relation = table.relation.clone();
        let flush_handle = tokio::spawn(async move {
            flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                    &[&relation],
                )
                .await
                .map_err(anyhow::Error::from)
        });

        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let hot_while_blocked = common::hot_row_count(&db.client, &table.relation).await?;
        assert!(
            hot_while_blocked >= 24,
            "hot rows must remain while fence waits on writer"
        );
        assert!(
            !flush_handle.is_finished(),
            "flush should still be waiting on SHARE ROW EXCLUSIVE"
        );

        blocker.batch_execute("ROLLBACK").await?;
        let _job = flush_handle.await??;
        common::assert_no_active_jobs(&db.client, &table.relation).await?;
        common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;
    }

    Ok(())
}

/// Continuous DML on a second async table must not corrupt the flushed table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_table_wal_during_async_flush_keeps_target_correct() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping multi-table WAL fence test in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "multi_wal_fence").await?;
        let primary = db.create_indexed_items_table("multi_primary", 40).await?;
        let noise = db.create_indexed_items_table("multi_noise", 10).await?;
        db.manage_shared(&primary.relation, "id").await?;
        db.manage_shared(&noise.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;

        let stop = Arc::new(AtomicBool::new(false));
        let noise_rel = noise.relation.clone();
        let noise_peer = connect_peer(&db).await?;
        let stop_flag = Arc::clone(&stop);
        let noise_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut n = 0i64;
            while !stop_flag.load(Ordering::Relaxed) {
                n += 1;
                let id = 500_000 + n;
                noise_peer
                    .execute(
                        &format!(
                            "INSERT INTO {noise_rel} (id, account_id, title, qty, category) \
                             VALUES ($1, 1, $2, 1, 'noise') \
                             ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title"
                        ),
                        &[&id, &format!("noise-{n}")],
                    )
                    .await?;
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Ok(())
        });

        let flushed = db.flush_table(&primary.relation).await?;
        assert_eq!(flushed, 40);
        stop.store(true, Ordering::Relaxed);
        noise_handle.await??;

        common::assert_flush_pruned_hot_storage(&db.client, &primary.relation, 40).await?;
        common::assert_pk_unique(&db.client, &primary.relation, &["id"]).await?;
        let merged: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", primary.relation), &[])
            .await?
            .get(0);
        assert_eq!(merged, 40);
    }

    Ok(())
}

/// Apply failpoint during flush phase-0 must leave retryable state and succeed later.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_apply_failpoint_during_flush_recovers_on_retry() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping apply-failpoint-during-flush in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_apply_fp").await?;
        let table = db.create_indexed_items_table("apply_fp_items", 20).await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;

        // Keep lag in WAL: disable launcher restart for this session + database.
        let dbname: String = db
            .client
            .query_one("SELECT current_database()", &[])
            .await?
            .get(0);
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.internal_async_mirror_worker = off; \
                 SET koldstore.internal_async_mirror_worker = off"
            ))
            .await?;
        let _ = common::terminate_async_worker(&db.client).await?;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        db.client
            .batch_execute(&format!(
                "INSERT INTO {} (id, account_id, title, qty, category)
                 SELECT g, 1, 'late-' || g::text, 1, 'x' FROM generate_series(1000, 1010) g",
                table.relation
            ))
            .await?;
        let lagged: i64 = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*) FROM {} WHERE id BETWEEN 1000 AND 1010",
                    common::change_log_mirror_relation(&table.relation)
                ),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(lagged, 0, "late rows must remain unapplied before flush");

        db.client
            .batch_execute("SET koldstore.failpoint = 'error:async_mirror_apply';")
            .await?;
        let first = db
            .client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                &[&table.relation],
            )
            .await;
        // Always restore GUCs so pooled worker DBs stay usable.
        db.client
            .batch_execute(&format!(
                "SET koldstore.failpoint = ''; \
                 ALTER DATABASE \"{dbname}\" RESET koldstore.internal_async_mirror_worker; \
                 RESET koldstore.internal_async_mirror_worker"
            ))
            .await?;
        assert!(
            first.is_err(),
            "flush must fail closed when phase-0 apply hit failpoint"
        );

        common::wait_for_async_worker(&db.client).await?;
        let _ = common::wait_for_async_mirror(&db.client).await?;

        let flushed = db.flush_table(&table.relation).await?;
        assert!(flushed > 0, "retry flush must publish after apply recovery");
        common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;
        let merged: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get(0);
        assert_eq!(merged, 31);
    }

    Ok(())
}

/// Killing the async worker while flush is paused must not leave duplicate mirror rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_kill_during_flush_upload_leaves_consistent_mirror() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping worker-kill-during-flush in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_kill_worker").await?;
        let table = db
            .create_indexed_items_table("kill_worker_items", 30)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        common::wait_for_async_worker(&db.client).await?;

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;
        let flush_client = connect_peer(&db).await?;
        let relation = table.relation.clone();
        let flush_handle = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await?;
            let row = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                    &[&relation],
                )
                .await;
            flush_client
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            row.map_err(anyhow::Error::from)
        });

        wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await?;
        assert!(common::terminate_async_worker(&db.client).await?);

        barrier_unlock(&coordinator).await?;
        let _ = flush_handle.await?;

        // Ensure worker can restart and mirror stays unique by PK latest-state.
        common::wait_for_async_worker(&db.client).await?;
        let _ = common::wait_for_async_mirror(&db.client).await?;
        let mirror = common::change_log_mirror_relation(&table.relation);
        let dupes: i64 = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*) FROM (
                       SELECT id FROM {mirror} GROUP BY id HAVING count(*) FILTER (WHERE op <> 3) > 1
                     ) d"
                ),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(dupes, 0, "mirror must not accumulate duplicate live rows");
    }

    Ok(())
}
