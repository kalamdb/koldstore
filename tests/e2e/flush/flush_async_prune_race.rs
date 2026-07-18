//! Concurrent DML during async flush must survive prune via the phase-6 fence.

use anyhow::{bail, Context, Result};
use tokio::task::JoinHandle;
use tokio_postgres::Row;

use crate::common;
use crate::flush::harness::{
    barrier_lock, barrier_unlock, connect_peer, wait_until_barrier_waiter, WORKER_COUNT,
};

async fn seed_async_table(db: &common::TestDb, table_name: &str, rows: i64) -> Result<String> {
    let relation = db.relation(table_name);
    let mode = common::selected_mirror_capture_mode()?.as_str();
    db.client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              id bigint PRIMARY KEY,
              body text NOT NULL
            );
            "#
        ))
        .await?;
    // Disable auto_flush so the background scheduler cannot race the failpoint
    // prune window under test.
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => NULL,
              migration_order_by => $3,
              auto_flush => false,
              mirror_capture_mode => $4
            )
            "#,
            &[&relation, &db.storage_name, &"id", &mode],
        )
        .await?;
    common::assert_system_columns_absent(&db.client, &relation).await?;
    common::assert_change_log_mirror_exists(&db.client, &format!("koldstore.{table_name}__cl"))
        .await?;
    common::assert_catalog_has_active_schema(&db.client, &relation).await?;
    db.client
        .batch_execute(&format!(
            r#"
            INSERT INTO {relation} (id, body)
            SELECT g, 'seed-' || g::text FROM generate_series(1, {rows}) AS g;
            "#
        ))
        .await?;
    common::fence_async_mirror_if_needed(&db.client).await?;
    Ok(relation)
}

async fn pause_flush_after_manifest(
    db: &common::TestDb,
    relation: &str,
) -> Result<(tokio_postgres::Client, JoinHandle<Result<Row>>)> {
    let coordinator = connect_peer(db).await?;
    barrier_lock(&coordinator).await?;

    let flush_client = connect_peer(db).await?;
    let flush_relation = relation.to_string();
    let flush_handle: JoinHandle<Result<Row>> = tokio::spawn(async move {
        flush_client
            .batch_execute("SET koldstore.failpoint = 'wait:after_manifest_publish';")
            .await?;
        let row = flush_client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                &[&flush_relation],
            )
            .await
            .context("flush_table during prune-race pause")?;
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

/// Reproduces the async flush prune race: UPDATE a selected PK after manifest
/// publish and before prune. The writer fence + bounded apply must keep the
/// newer hot row instead of deleting it with the stale mirror watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_during_async_flush_prune_keeps_newer_hot_row() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async flush prune fence race in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_prune_race").await?;
        let relation = seed_async_table(&db, "prune_race_items", 32).await?;
        let mirror = common::change_log_mirror_relation(&relation);

        let max_seq_before: i64 = db
            .client
            .query_one(
                &format!("SELECT COALESCE(max(seq), 0)::bigint FROM {mirror}"),
                &[],
            )
            .await?
            .get(0);

        let (coordinator, flush_handle) = pause_flush_after_manifest(&db, &relation).await?;

        let peer = connect_peer(&db).await?;
        peer.execute(
            &format!("UPDATE {relation} SET body = 'raced-update' WHERE id = 5"),
            &[],
        )
        .await
        .context("concurrent update during flush prune window")?;

        barrier_unlock(&coordinator).await?;
        let _job = flush_handle.await??;
        let _ = db
            .client
            .batch_execute("SET koldstore.failpoint = '';")
            .await;

        let hot = db
            .client
            .query_opt(&format!("SELECT body FROM {relation} WHERE id = 5"), &[])
            .await?;
        let Some(hot) = hot else {
            bail!("expected id=5 to remain hot after async flush prune race");
        };
        assert_eq!(hot.get::<_, String>(0), "raced-update");

        let mirror_row = db
            .client
            .query_one(
                &format!(
                    "SELECT seq::bigint, op::smallint FROM {mirror} \
                     WHERE id = 5 ORDER BY seq DESC LIMIT 1"
                ),
                &[],
            )
            .await?;
        let seq: i64 = mirror_row.get(0);
        assert!(seq > max_seq_before, "mirror seq must exceed watermark");
        assert_eq!(mirror_row.get::<_, i16>(1), 2);
    }

    Ok(())
}

/// DELETE during the publish→prune window must leave a tombstone that masks cold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_during_async_flush_prune_keeps_tombstone() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async prune DELETE race in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_prune_del").await?;
        let relation = seed_async_table(&db, "prune_del_items", 32).await?;
        let mirror = common::change_log_mirror_relation(&relation);
        let max_seq_before: i64 = db
            .client
            .query_one(
                &format!("SELECT COALESCE(max(seq), 0)::bigint FROM {mirror}"),
                &[],
            )
            .await?
            .get(0);

        let (coordinator, flush_handle) = pause_flush_after_manifest(&db, &relation).await?;
        let peer = connect_peer(&db).await?;
        peer.execute(&format!("DELETE FROM {relation} WHERE id = 5"), &[])
            .await
            .context("concurrent delete during flush prune window")?;

        barrier_unlock(&coordinator).await?;
        let _job = flush_handle.await??;
        let _ = db
            .client
            .batch_execute("SET koldstore.failpoint = '';")
            .await;

        let visible = db
            .client
            .query_opt(&format!("SELECT 1 FROM {relation} WHERE id = 5"), &[])
            .await?;
        assert!(
            visible.is_none(),
            "deleted id=5 must stay masked after prune fence"
        );

        let mirror_row = db
            .client
            .query_opt(
                &format!(
                    "SELECT seq::bigint, op::smallint FROM {mirror} \
                     WHERE id = 5 ORDER BY seq DESC LIMIT 1"
                ),
                &[],
            )
            .await?
            .with_context(|| {
                format!("expected mirror tombstone for id=5 in {mirror} after prune fence")
            })?;
        assert!(mirror_row.get::<_, i64>(0) > max_seq_before);
        assert_eq!(mirror_row.get::<_, i16>(1), 3, "expected delete op");
    }

    Ok(())
}

/// DELETE then re-INSERT the same PK during the fence window must keep the new row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reinsert_during_async_flush_prune_keeps_new_row() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async prune reinsert race in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_prune_reins").await?;
        let relation = seed_async_table(&db, "prune_reins_items", 32).await?;

        let (coordinator, flush_handle) = pause_flush_after_manifest(&db, &relation).await?;
        let peer = connect_peer(&db).await?;
        peer.batch_execute(&format!(
            "DELETE FROM {relation} WHERE id = 5; INSERT INTO {relation} (id, body) VALUES (5, 'reinserted');"
        ))
        .await
        .context("concurrent delete+reinsert during flush prune window")?;

        barrier_unlock(&coordinator).await?;
        let _job = flush_handle.await??;
        let _ = db
            .client
            .batch_execute("SET koldstore.failpoint = '';")
            .await;

        let body: String = db
            .client
            .query_one(&format!("SELECT body FROM {relation} WHERE id = 5"), &[])
            .await?
            .get(0);
        assert_eq!(body, "reinserted");
    }

    Ok(())
}

/// High DML during the publish→prune window must not lose newer hot winners.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn firehose_during_async_flush_prune_preserves_hot_winners() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async prune firehose in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_prune_fire").await?;
        let relation = seed_async_table(&db, "prune_fire_items", 64).await?;

        let (coordinator, flush_handle) = pause_flush_after_manifest(&db, &relation).await?;

        let mut workers = Vec::new();
        for worker_id in 0..WORKER_COUNT {
            let peer = connect_peer(&db).await?;
            let rel = relation.clone();
            workers.push(tokio::spawn(async move {
                for seq in 0..10i64 {
                    let id = 1_000_000i64 + (worker_id as i64) * 10_000 + seq;
                    peer.execute(
                        &format!("INSERT INTO {rel} (id, body) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body"),
                        &[&id, &format!("w{worker_id}-{seq}")],
                    )
                    .await?;
                    if seq % 3 == 0 {
                        peer.execute(
                            &format!("UPDATE {rel} SET body = body || '-u' WHERE id = $1"),
                            &[&id],
                        )
                        .await?;
                    }
                }
                Ok::<(), anyhow::Error>(())
            }));
        }

        for handle in workers {
            handle.await??;
        }

        barrier_unlock(&coordinator).await?;
        let _job = flush_handle.await??;
        let _ = db
            .client
            .batch_execute("SET koldstore.failpoint = '';")
            .await;

        common::assert_pk_unique(&db.client, &relation, &["id"]).await?;
        let hot_new: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*) FROM {relation} WHERE id >= 1000000"),
                &[],
            )
            .await?
            .get(0);
        assert!(
            hot_new > 0,
            "firehose inserts during prune window must remain visible"
        );
    }

    Ok(())
}
