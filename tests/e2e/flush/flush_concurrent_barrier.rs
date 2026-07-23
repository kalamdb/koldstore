//! Barrier-synchronized concurrent flush: DML/queries overlap mid-flush.

use anyhow::Result;
use tokio::task::JoinHandle;
use tokio_postgres::Row;

use crate::common;
use crate::flush::harness::{
    assert_flush_load_invariants, barrier_lock, barrier_unlock, connect_peer, connect_workers,
    join_workers, spawn_barrier_workers, wait_until_barrier_waiter, BARRIER_WORKER_LOOPS,
    WORKER_COUNT,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_barrier_overlaps_ten_mixed_workers() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_bar").await?;
        let table = db.create_indexed_items_table("barrier_items", 96).await?;
        db.manage_shared(&table.relation, "id").await?;

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = table.relation.clone();
        let flush_handle: JoinHandle<Result<Option<Row>>> = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await?;
            // Concurrent DML while paused can produce selection/write mismatch;
            // isolation schedules use the same tolerate-then-clean-flush pattern.
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
                Ok(row) => Ok(Some(row)),
                Err(error) => {
                    let detail = error
                        .as_db_error()
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| error.to_string());
                    if detail.contains("selection mismatch")
                        || detail.contains("failpoint")
                        || detail.contains("flush")
                    {
                        Ok(None)
                    } else {
                        Err(error.into())
                    }
                }
            }
        });

        // Workers must start only after flush has selected rows and parked, so
        // concurrent mutations overlap mid-flush rather than racing selection.
        wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await?;

        let peers = connect_workers(&db, WORKER_COUNT).await?;
        let workers = spawn_barrier_workers(peers, table.relation.clone(), BARRIER_WORKER_LOOPS);
        join_workers(workers).await?;

        barrier_unlock(&coordinator).await?;
        let first = flush_handle.await??;

        db.client
            .batch_execute("SET koldstore.failpoint = '';")
            .await
            .ok();
        // Multi-wave catch-up may finish draining inside the paused flush after
        // unlock; otherwise a clean follow-up archives leftover mirror rows.
        let cleaned = db.flush_table(&table.relation).await?;
        assert!(
            first.is_some() || cleaned > 0,
            "expected paused flush and/or cleanup flush to archive rows \
             (first_completed={}, cleaned={cleaned})",
            first.is_some()
        );

        assert_flush_load_invariants(&db.client, &table.relation).await?;
        // Worker inserts live in id bands starting at 1_000_000 (PK-indexed cold filter).
        let sample = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM {} WHERE id >= 1000000 AND id < 1100000",
                    table.relation
                ),
                &[],
            )
            .await?;
        let worker_rows: i64 = sample.get(0);
        assert!(
            worker_rows > 0,
            "expected concurrent workers to leave visible inserts"
        );
    }

    Ok(())
}
