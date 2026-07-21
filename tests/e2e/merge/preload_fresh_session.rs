//! Regression: fresh sessions must see cold rows without prior koldstore SQL.
//!
//! Without `shared_preload_libraries = 'koldstore'`, planner hooks exist only
//! in backends that already loaded the `.so`. Beekeeper / app pools then fall
//! back to heap Seq Scan and return hot-only counts after flush.

use anyhow::{Context, Result};

use crate::common;
use crate::flush::harness::connect_peer;

#[tokio::test]
async fn fresh_session_after_flush_uses_merge_scan_and_sees_cold_rows() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "preload_fresh").await?;

        let preload: String = db
            .client
            .query_one("SHOW shared_preload_libraries", &[])
            .await
            .context("SHOW shared_preload_libraries")?
            .get(0);
        anyhow::ensure!(
            preload
                .split(',')
                .map(str::trim)
                .any(|entry| entry == "koldstore"),
            "e2e server must shared-preload koldstore, got '{preload}'"
        );

        let table = db.create_indexed_items_table("messages", 64).await?;
        db.manage_shared(&table.relation, "id").await?;
        db.client
            .execute(
                "SELECT koldstore.set_table_auto_flush($1::text::regclass, false)",
                &[&table.relation],
            )
            .await?;
        common::fence_async_mirror_if_needed(&db.client).await?;

        let flushed = db.flush_table(&table.relation).await?;
        anyhow::ensure!(flushed > 0, "expected flush to archive rows");
        common::assert_no_active_jobs(&db.client, &table.relation).await?;

        let status = common::describe_table(&db.client, &table.relation).await?;
        anyhow::ensure!(
            status.cold_row_count > 0,
            "expected cold rows after flush, status={status:?}"
        );
        let expected = status.hot_rows + status.cold_row_count;

        // Brand-new backend: open peer before any koldstore.* on that connection.
        // preload_status / EXPLAIN / count are the first statements on `peer`.
        let peer = connect_peer(&db).await?;

        let peer_preload: String = peer
            .query_one("SHOW shared_preload_libraries", &[])
            .await
            .context("SHOW shared_preload on peer")?
            .get(0);
        anyhow::ensure!(
            peer_preload
                .split(',')
                .map(str::trim)
                .any(|entry| entry == "koldstore"),
            "fresh peer must inherit shared_preload, got '{peer_preload}'"
        );

        let plan =
            common::explain(&peer, &format!("SELECT count(*) FROM {}", table.relation)).await?;
        anyhow::ensure!(
            plan.contains("KoldMergeScan") || plan.contains("Custom Scan"),
            "fresh session must use KoldMergeScan, got: {plan}"
        );

        let count: i64 = peer
            .query_one(
                &format!("SELECT count(*)::bigint FROM {}", table.relation),
                &[],
            )
            .await
            .context("count on fresh peer")?
            .get(0);
        anyhow::ensure!(
            count == expected,
            "fresh session count must include cold rows: got {count}, expected {expected} (hot={} cold={})",
            status.hot_rows,
            status.cold_row_count
        );

        let preload_status: serde_json::Value = {
            let status_row = peer
                .query_one("SELECT koldstore.preload_status()::text", &[])
                .await
                .context("preload_status on fresh peer")?;
            serde_json::from_str(&status_row.get::<_, String>(0))
                .context("parse preload_status json")?
        };
        anyhow::ensure!(
            preload_status.get("loaded_via_shared_preload") == Some(&serde_json::Value::Bool(true)),
            "preload_status must report loaded_via_shared_preload: {preload_status}"
        );
    }
    Ok(())
}
