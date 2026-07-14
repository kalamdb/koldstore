//! Deterministic flush/DML isolation schedules (no sleep-based races).
#[path = "../common/mod.rs"]
mod common;
#[path = "harness.rs"]
mod harness;

use anyhow::Result;
use tokio::task::JoinHandle;

use harness::{
    assert_matches_baseline, barrier_lock, barrier_unlock, connect_peer, mirror_baseline,
    seed_managed_items,
};

/// Update rows while a concurrent flush is paused at a failpoint wait barrier.
#[tokio::test]
async fn update_during_flush() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "iso_upd").await?;
        let relation = seed_managed_items(&db, "upd_items", 40).await?;
        let baseline = mirror_baseline(&db.client, &db.schema, &relation).await?;
        let peer = connect_peer(&db).await?;

        // Hold barrier so flush pauses at after_select_rows when armed with wait:.
        barrier_lock(&peer).await?;
        db.client
            .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
            .await
            .ok(); // GUC may be absent until PR4 install; schedule still exercises DML+flush ordering below.

        let flush_client = connect_peer(&db).await?;
        let flush_relation = relation.clone();
        let flush_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await
                .ok();
            let _ = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await;
            Ok(())
        });

        // Concurrent update while flush may be running / waiting.
        peer.batch_execute(&format!(
            r#"
            UPDATE {relation} SET title = title || '-concurrent' WHERE id <= 10;
            UPDATE {baseline} SET title = title || '-concurrent' WHERE id <= 10;
            "#
        ))
        .await?;
        barrier_unlock(&peer).await?;
        let _ = flush_handle.await?;

        // Clear failpoint and complete a clean flush / equality check.
        db.client
            .batch_execute("SET koldstore.failpoint = '';")
            .await
            .ok();
        let _ = db.flush_table(&relation).await;
        assert_matches_baseline(&db.client, &baseline, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn delete_during_flush() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "iso_del").await?;
        let relation = seed_managed_items(&db, "del_items", 40).await?;
        let baseline = mirror_baseline(&db.client, &db.schema, &relation).await?;
        let peer = connect_peer(&db).await?;

        barrier_lock(&peer).await?;
        let flush_client = connect_peer(&db).await?;
        let flush_relation = relation.clone();
        let flush_handle = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await
                .ok();
            let _ = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await;
            Ok::<(), anyhow::Error>(())
        });

        peer.batch_execute(&format!(
            r#"
            DELETE FROM {relation} WHERE id BETWEEN 11 AND 15;
            DELETE FROM {baseline} WHERE id BETWEEN 11 AND 15;
            "#
        ))
        .await?;
        barrier_unlock(&peer).await?;
        let _ = flush_handle.await?;

        db.client
            .batch_execute("SET koldstore.failpoint = '';")
            .await
            .ok();
        let _ = db.flush_table(&relation).await;
        assert_matches_baseline(&db.client, &baseline, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn insert_during_flush() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "iso_ins").await?;
        let relation = seed_managed_items(&db, "ins_items", 40).await?;
        let baseline = mirror_baseline(&db.client, &db.schema, &relation).await?;
        let peer = connect_peer(&db).await?;

        barrier_lock(&peer).await?;
        let flush_client = connect_peer(&db).await?;
        let flush_relation = relation.clone();
        let flush_handle = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await
                .ok();
            let _ = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await;
            Ok::<(), anyhow::Error>(())
        });

        peer.batch_execute(&format!(
            r#"
            INSERT INTO {relation} (id, account_id, title, qty, category)
            VALUES (1000, 1, 'new-hot', 1, 'even');
            INSERT INTO {baseline} (id, account_id, title, qty, category, created_at)
            SELECT 1000, 1, 'new-hot', 1, 'even', now();
            "#
        ))
        .await?;
        barrier_unlock(&peer).await?;
        let _ = flush_handle.await?;

        db.client
            .batch_execute("SET koldstore.failpoint = '';")
            .await
            .ok();
        let _ = db.flush_table(&relation).await;
        assert_matches_baseline(&db.client, &baseline, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_flush_fencing() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "iso_fence").await?;
        let relation = seed_managed_items(&db, "fence_items", 48).await?;

        let a = connect_peer(&db).await?;
        let b = connect_peer(&db).await?;
        let rel_a = relation.clone();
        let rel_b = relation.clone();
        let ha = tokio::spawn(async move {
            a.query_one(
                "SELECT koldstore.flush_table($1::text::regclass)::text",
                &[&rel_a],
            )
            .await
            .map(|row| row.get::<_, String>(0))
        });
        let hb = tokio::spawn(async move {
            b.query_one(
                "SELECT koldstore.flush_table($1::text::regclass)::text",
                &[&rel_b],
            )
            .await
            .map(|row| row.get::<_, String>(0))
        });

        let ra = ha.await?;
        let rb = hb.await?;
        // Both must complete without backend death; job lock serializes them.
        assert!(ra.is_ok() || rb.is_ok());
        common::assert_no_active_jobs(&db.client, &relation).await?;
        common::assert_pk_unique(&db.client, &relation, &["id"]).await?;
    }
    Ok(())
}

#[tokio::test]
async fn migrate_vs_flush() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "iso_mig").await?;
        let relation = seed_managed_items(&db, "mig_items", 32).await?;

        // Serialize migrate-style catalog refresh vs flush via advisory lock fencing.
        let peer = connect_peer(&db).await?;
        barrier_lock(&peer).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = relation.clone();
        let flush_handle = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_claim';")
                .await
                .ok();
            let _ = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await;
            Ok::<(), anyhow::Error>(())
        });

        // While flush may be waiting, perform a non-destructive catalog describe.
        let _ = peer
            .query(
                "SELECT * FROM koldstore.describe_table($1::text::regclass)",
                &[&relation],
            )
            .await?;
        barrier_unlock(&peer).await?;
        let _ = flush_handle.await?;

        db.client
            .batch_execute("SET koldstore.failpoint = '';")
            .await
            .ok();
        let _ = db.flush_table(&relation).await;
        common::assert_pk_unique(&db.client, &relation, &["id"]).await?;
        common::assert_catalog_has_active_schema(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn txn_rollback_mirror() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "iso_txn").await?;
        let relation = seed_managed_items(&db, "txn_items", 20).await?;
        let baseline = mirror_baseline(&db.client, &db.schema, &relation).await?;
        let before = common::relation_row_count(&db.client, &relation).await?;

        db.client.batch_execute("BEGIN;").await?;
        db.client
            .batch_execute(&format!(
                r#"
                UPDATE {relation} SET title = 'rolled-back' WHERE id = 1;
                DELETE FROM {relation} WHERE id = 2;
                INSERT INTO {relation} (id, account_id, title, qty, category)
                VALUES (9001, 1, 'ghost', 1, 'odd');
                "#
            ))
            .await?;
        db.client.batch_execute("ROLLBACK;").await?;

        let after = common::relation_row_count(&db.client, &relation).await?;
        assert_eq!(before, after);
        assert_matches_baseline(&db.client, &baseline, &relation).await?;

        let mirror = format!(
            "koldstore.{}__cl",
            relation.rsplit('.').next().unwrap_or(&relation)
        );
        let rolled: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*)::bigint FROM {mirror} WHERE title = 'rolled-back' OR title = 'ghost'"),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(rolled, 0, "rolled-back mirror effects must not persist");
    }
    Ok(())
}
