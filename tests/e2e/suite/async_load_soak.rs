//! Sustained async DML + periodic flush soak (env-gated duration).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinHandle;

use crate::common;
use crate::flush::harness::connect_peer;

fn soak_duration() -> Duration {
    if std::env::var("KOLDSTORE_E2E_SOAK")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes"))
        .unwrap_or(false)
    {
        let secs = std::env::var("KOLDSTORE_E2E_SOAK_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(45);
        Duration::from_secs(secs)
    } else {
        // Default short soak so the suite stays fast unless opted in.
        Duration::from_secs(3)
    }
}

/// Concurrent INSERT/UPDATE/DELETE/SELECT + periodic flush on multiple async tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_mixed_load_soak_keeps_invariants() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async load soak in strict mode");
        return Ok(());
    }
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_soak").await?;
        let tables = [
            db.create_indexed_items_table("soak_a", 30).await?,
            db.create_indexed_items_table("soak_b", 30).await?,
            db.create_indexed_items_table("soak_c", 30).await?,
        ];
        for table in &tables {
            db.manage_shared(&table.relation, "id").await?;
        }
        common::fence_async_mirror_if_needed(&db.client).await?;

        let stop = Arc::new(AtomicBool::new(false));
        let mut workers: Vec<JoinHandle<Result<()>>> = Vec::new();

        for (worker_id, table) in tables.iter().enumerate() {
            let peer = connect_peer(&db).await?;
            let relation = table.relation.clone();
            let stop_flag = Arc::clone(&stop);
            workers.push(tokio::spawn(async move {
                let mut seq = 0i64;
                while !stop_flag.load(Ordering::Relaxed) {
                    seq += 1;
                    let id = 2_000_000 + (worker_id as i64) * 100_000 + (seq % 500);
                    match seq % 5 {
                        0 => {
                            peer.execute(
                                &format!(
                                    "INSERT INTO {relation} (id, account_id, title, qty, category) \
                                     VALUES ($1, 1, $2, 1, 'soak') \
                                     ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title"
                                ),
                                &[&id, &format!("soak-{worker_id}-{seq}")],
                            )
                            .await?;
                        }
                        1 => {
                            peer.execute(
                                &format!(
                                    "UPDATE {relation} SET title = title || '-u' WHERE id = $1"
                                ),
                                &[&id],
                            )
                            .await
                            .ok();
                        }
                        2 => {
                            peer.execute(&format!("DELETE FROM {relation} WHERE id = $1"), &[&id])
                                .await
                                .ok();
                        }
                        _ => {
                            let _ = peer
                                .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
                                .await?;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Ok(())
            }));
        }

        let flush_peer = connect_peer(&db).await?;
        let flush_relations: Vec<String> = tables.iter().map(|t| t.relation.clone()).collect();
        let stop_flush = Arc::clone(&stop);
        let flush_worker = tokio::spawn(async move {
            let mut i = 0usize;
            while !stop_flush.load(Ordering::Relaxed) {
                let relation = &flush_relations[i % flush_relations.len()];
                let _ = flush_peer
                    .query_one(
                        "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                        &[relation],
                    )
                    .await;
                i += 1;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok::<(), anyhow::Error>(())
        });

        tokio::time::sleep(soak_duration()).await;
        stop.store(true, Ordering::Relaxed);
        for worker in workers {
            worker.await??;
        }
        flush_worker.await??;

        common::fence_async_mirror_if_needed(&db.client).await?;
        for table in &tables {
            common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;
            common::assert_no_active_jobs(&db.client, &table.relation).await?;
            let count = common::row_count(&db.client, &table.relation).await?;
            anyhow::ensure!(count > 0, "{} must still have visible rows", table.relation);
        }

        common::log_always(format!(
            "async soak completed in {:?} across {} tables",
            soak_duration(),
            tables.len()
        ));
    }

    Ok(())
}
