//! Production-shaped hot/cold gap coverage inspired by TimescaleDB compression
//! DML/isolation tests and Iceberg/ClickHouse fail-closed concurrent reads.
//!
//! These cases intentionally avoid duplicating flush prune races, worker kill,
//! single-segment missing parquet, and soak already covered elsewhere.

use anyhow::{Context, Result};
use tokio::task::JoinHandle;
use tokio_postgres::Row;

use crate::common;
use crate::flush::harness::{
    barrier_lock, barrier_unlock, connect_peer, wait_until_barrier_waiter,
};

async fn active_segment_paths(db: &common::TestDb, relation: &str) -> Result<Vec<String>> {
    let rows = db
        .client
        .query(
            r#"
            SELECT cs.object_path
            FROM koldstore.cold_segments cs
            WHERE cs.table_oid = $1::text::regclass::oid
              AND cs.status = 'active'
            ORDER BY cs.batch_number, cs.created_at
            "#,
            &[&relation],
        )
        .await
        .context("list active cold segments")?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn pause_flush_at(
    db: &common::TestDb,
    relation: &str,
    failpoint: &str,
) -> Result<(tokio_postgres::Client, JoinHandle<Result<Row>>)> {
    let coordinator = connect_peer(db).await?;
    barrier_lock(&coordinator).await?;
    let flush_client = connect_peer(db).await?;
    let flush_relation = relation.to_string();
    let armed = failpoint.to_string();
    let flush_handle: JoinHandle<Result<Row>> = tokio::spawn(async move {
        flush_client
            .batch_execute(&format!("SET koldstore.failpoint = '{armed}';"))
            .await?;
        let row = flush_client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                &[&flush_relation],
            )
            .await
            .context("flush_table during tiered stress pause")?;
        flush_client
            .batch_execute("SET koldstore.failpoint = '';")
            .await
            .ok();
        Ok(row)
    });
    if let Err(error) = wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await
    {
        barrier_unlock(&coordinator).await.ok();
        let _ = flush_handle.await;
        return Err(error);
    }
    Ok((coordinator, flush_handle))
}

/// Iceberg-style: after two publishes, losing one active segment must fail closed
/// (not silently return a partial hot/cold mix).
#[tokio::test]
async fn multi_generation_missing_one_segment_fails_merge_closed() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "multi_seg_outage").await?;
        let table = db.create_indexed_items_table("multi_seg_items", 12).await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        // Second generation: new hot rows, then another flush (two active segments).
        db.client
            .batch_execute(&format!(
                "INSERT INTO {} (id, account_id, title, qty, category)
                 SELECT g, 1, 'gen2-' || g::text, 1, 'x'
                 FROM generate_series(1000, 1011) g",
                table.relation
            ))
            .await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        let paths = active_segment_paths(&db, &table.relation).await?;
        assert!(
            paths.len() >= 2,
            "expected >=2 active segments after two flushes, got {}",
            paths.len()
        );
        let victim = db.storage_root.join(&paths[1]);
        std::fs::remove_file(&victim).with_context(|| format!("remove {}", victim.display()))?;

        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await
            .expect_err("merge must fail closed when one of N segments is missing");
        assert!(err.as_db_error().is_some());
    }
    Ok(())
}

/// Timescale/Iceberg concurrent-read pattern: readers during the publish→prune
/// window must see a consistent PK set (no torn duplicate cold+hot winners).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_during_manifest_publish_window_keeps_pk_unique() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "publish_window_read").await?;
        let table = db
            .create_indexed_items_table("publish_window_items", 40)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;

        let (coordinator, flush_handle) =
            pause_flush_at(&db, &table.relation, "wait:after_manifest_publish").await?;

        let reader = connect_peer(&db).await?;
        let count: i64 = reader
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get(0);
        assert_eq!(
            count, 40,
            "publish-window SELECT must keep the full committed row set"
        );
        common::assert_pk_unique(&reader, &table.relation, &["id"]).await?;

        barrier_unlock(&coordinator).await?;
        let _ = flush_handle.await??;
        common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;
        assert_eq!(common::row_count(&db.client, &table.relation).await?, 40);
    }
    Ok(())
}

/// Committed hot inserts must be visible to merge reads even when the async
/// mirror worker is stopped (Timescale: readers must not wait on compression).
#[tokio::test]
async fn committed_hot_visible_while_async_mirror_lags() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async lag visibility test in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_lag_read").await?;
        let table = db.create_indexed_items_table("lag_items", 16).await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 16).await?;

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
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        db.client
            .batch_execute(&format!(
                "INSERT INTO {} (id, account_id, title, qty, category)
                 VALUES (9001, 1, 'lag-hot', 1, 'hot')",
                table.relation
            ))
            .await?;

        // Do NOT call wait_for_async_mirror — lag is intentional.
        let title: String = db
            .client
            .query_one(
                &format!("SELECT title FROM {} WHERE id = 9001", table.relation),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(title, "lag-hot");
        let count: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get(0);
        assert_eq!(
            count, 17,
            "merge must include lagging hot without mirror catch-up"
        );

        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" RESET koldstore.internal_async_mirror_worker; \
                 RESET koldstore.internal_async_mirror_worker"
            ))
            .await?;
    }
    Ok(())
}

/// Timescale decompress-modify-recompress analog: rematerialize a cold PK,
/// UPDATE it, flush again, and keep the latest overlay visible.
#[tokio::test]
async fn rematerialize_update_and_reflush_keeps_latest_overlay() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "rematerialize_reflush").await?;
        let table = db
            .create_indexed_items_table("remat_reflush_items", 16)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 16).await?;

        // Cold-only SQL UPDATE is a no-op in MVP; rematerialize then update (Timescale path).
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, account_id, title, qty, category)
                VALUES (5, 1, 'rematerialized', 5, 'hot')
                ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDED.title, qty = EXCLUDED.qty, category = EXCLUDED.category;
                UPDATE {relation} SET title = 'after-reflush', qty = 55 WHERE id = 5;
                "#,
                relation = table.relation
            ))
            .await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        let row = db
            .client
            .query_one(
                &format!("SELECT title, qty FROM {} WHERE id = 5", table.relation),
                &[],
            )
            .await?;
        assert_eq!(row.get::<_, String>(0), "after-reflush");
        assert_eq!(row.get::<_, i32>(1), 55);
        assert_eq!(common::row_count(&db.client, &table.relation).await?, 16);
        common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;
    }
    Ok(())
}

/// Timescale prepared-plan invalidation: a prepared merge SELECT must remain
/// correct after flush prunes hot and publishes cold.
#[tokio::test]
async fn prepared_merge_select_stays_correct_across_flush() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "prepared_merge").await?;
        let table = db
            .create_indexed_items_table("prepared_merge_items", 18)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;

        db.client
            .batch_execute(&format!(
                "PREPARE merge_count AS SELECT count(*)::bigint FROM {}",
                table.relation
            ))
            .await?;
        let before: i64 = db
            .client
            .query_one("EXECUTE merge_count", &[])
            .await?
            .get(0);
        assert_eq!(before, 18);

        assert!(db.flush_table(&table.relation).await? > 0);
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 18).await?;

        let after: i64 = db
            .client
            .query_one("EXECUTE merge_count", &[])
            .await?
            .get(0);
        assert_eq!(
            after, 18,
            "prepared merge plan must still see all rows after flush prune"
        );
        db.client.batch_execute("DEALLOCATE merge_count").await?;
    }
    Ok(())
}
