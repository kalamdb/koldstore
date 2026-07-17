//! Firehose concurrent flush: 10 mixed DML/query peers while flush runs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;

use crate::common;
use crate::flush::harness::{
    assert_flush_load_invariants, connect_peer, connect_workers, flush_table_on, join_workers,
    spawn_firehose_workers, WORKER_COUNT,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_while_ten_mixed_workers_write_and_query() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_load").await?;
        let table = db.create_indexed_items_table("load_items", 128).await?;
        db.manage_shared(&table.relation, "id").await?;

        let stop = Arc::new(AtomicBool::new(false));
        let peers = connect_workers(&db, WORKER_COUNT).await?;
        let workers = spawn_firehose_workers(peers, table.relation.clone(), Arc::clone(&stop));

        // Give workers a moment to start issuing statements before flush.
        tokio::task::yield_now().await;

        // Flush on a dedicated peer so a deadlock abort does not poison db.client.
        let flush_peer = connect_peer(&db).await?;
        let mut overlapped = false;
        for attempt in 1..=5 {
            match flush_table_on(&flush_peer, &table.relation).await {
                Ok(rows) if rows > 0 => {
                    overlapped = true;
                    break;
                }
                Ok(_) => {
                    eprintln!("flush attempt {attempt} completed with 0 rows; retrying");
                }
                Err(error) if is_transient_flush_error(&error) => {
                    eprintln!(
                        "flush attempt {attempt} hit transient concurrency error; retrying: {error:#}"
                    );
                    let _ = flush_peer.batch_execute("ROLLBACK").await;
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(error) => {
                    let _ = flush_peer.batch_execute("ROLLBACK").await;
                    return Err(error);
                }
            }
        }

        stop.store(true, Ordering::Relaxed);
        join_workers(workers).await?;

        // Always finish with a clean post-load flush on the primary client.
        let cleaned = db.flush_table(&table.relation).await?;
        assert!(
            overlapped || cleaned > 0,
            "expected overlapping and/or cleanup flush to archive rows \
             (overlapped={overlapped}, cleaned={cleaned})"
        );
        if cleaned == 0 && overlapped {
            // Concurrent flush already archived seed rows; cleanup may be a no-op.
        } else if cleaned == 0 {
            anyhow::bail!("cleanup flush archived no rows after concurrent load");
        }

        assert_flush_load_invariants(&db.client, &table.relation).await?;

        let plan = common::explain(
            &db.client,
            &format!(
                "SELECT id, title FROM {} WHERE id IN (1, 2, 3)",
                table.relation
            ),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan)?;
    }

    Ok(())
}

fn is_transient_flush_error(error: &anyhow::Error) -> bool {
    let text = format!("{error:#}");
    // Object-key collisions after a rolled-back publish are a product bug, not a
    // soft concurrency blip — unique segment paths must make retries safe.
    if text.contains("object validation failed") {
        return false;
    }
    text.contains("deadlock detected")
        || text.contains("selection mismatch")
        || text.contains("could not serialize")
        || text.contains("status=error")
}
