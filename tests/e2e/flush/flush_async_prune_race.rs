//! Concurrent UPDATE during async flush must survive prune via the phase-6 fence.

use anyhow::{bail, Context, Result};
use tokio::task::JoinHandle;
use tokio_postgres::Row;

use crate::common;
use crate::flush::harness::{barrier_lock, barrier_unlock, connect_peer};

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
        let table_name = "prune_race_items";
        let relation = db.relation(table_name);
        let mirror = common::change_log_mirror_relation(&relation);

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
        db.manage_shared(&relation, "id").await?;
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, body)
                SELECT g, 'seed-' || g::text FROM generate_series(1, 32) AS g;
                "#
            ))
            .await?;
        common::fence_async_mirror_if_needed(&db.client).await?;

        let max_seq_before: i64 = db
            .client
            .query_one(
                &format!("SELECT COALESCE(max(seq), 0)::bigint FROM {mirror}"),
                &[],
            )
            .await?
            .get(0);

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = relation.clone();
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

        // Wait until flush is blocked on the failpoint barrier.
        let mut paused = false;
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
                    &[&crate::flush::harness::BARRIER_LOCK_KEY],
                )
                .await?
                .get::<_, bool>(0);
            if waiting {
                paused = true;
                break;
            }
            if flush_handle.is_finished() {
                break;
            }
        }
        if !paused {
            barrier_unlock(&coordinator).await.ok();
            let _ = flush_handle.await;
            bail!("flush did not reach wait:after_manifest_publish");
        }

        // Concurrent commit while mirror still lags (apply lock held by flush).
        let peer = connect_peer(&db).await?;
        peer.execute(
            &format!("UPDATE {relation} SET body = 'raced-update' WHERE id = 5"),
            &[],
        )
        .await
        .context("concurrent update during flush prune window")?;

        barrier_unlock(&coordinator).await?;
        let _job = flush_handle.await??;

        db.client
            .batch_execute("SET koldstore.failpoint = '';")
            .await
            .ok();

        let hot = db
            .client
            .query_opt(&format!("SELECT body FROM {relation} WHERE id = 5"), &[])
            .await?;
        let Some(hot) = hot else {
            bail!("expected id=5 to remain hot after async flush prune race");
        };
        let body: String = hot.get(0);
        assert_eq!(
            body, "raced-update",
            "newer hot body must survive prune after concurrent async UPDATE"
        );

        let mirror_row = db
            .client
            .query_one(
                &format!("SELECT seq::bigint, op::smallint FROM {mirror} WHERE id = 5"),
                &[],
            )
            .await?;
        let seq: i64 = mirror_row.get(0);
        let op: i16 = mirror_row.get(1);
        assert!(
            seq > max_seq_before,
            "mirror seq for raced PK must exceed pre-flush watermark ({seq} <= {max_seq_before})"
        );
        assert_eq!(op, 2, "raced UPDATE should leave a live update mirror op");

        // Visible merged read must prefer the newer hot overlay.
        let merged: String = db
            .client
            .query_one(&format!("SELECT body FROM {relation} WHERE id = 5"), &[])
            .await?
            .get(0);
        assert_eq!(merged, "raced-update");
    }

    Ok(())
}
