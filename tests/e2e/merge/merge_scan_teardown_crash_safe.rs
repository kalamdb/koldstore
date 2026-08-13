//! Regression: KoldMergeScan teardown must not abort the backend.
//!
//! Docker load testing showed that after a managed hot+cold `count(*)`:
//! - fail-closed on `koldstore.max_merge_seen_keys`, or
//! - a successful unbounded scan (`max_merge_seen_keys = 0`),
//!
//! the **next statement on the same backend** could hit glibc
//! `double free or corruption` and terminate with signal 6.
//!
//! Root cause: portal ERROR skips `EndCustomScan` while `SCAN_STATES` still
//! owned a deleted AllocSet; success path also cleared slots after ScanMemory
//! drop. This e2e keeps one TCP session and asserts follow-up queries stay live.

use anyhow::{bail, Context, Result};

use crate::common;

#[tokio::test]
async fn merge_scan_followup_query_survives_seen_key_error_and_unbounded_count() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_teardown_safe").await?;
        let table = db.create_indexed_items_table("teardown_items", 500).await?;
        // Low hot_row_limit so force flush publishes cold and count(*) merges.
        db.client
            .batch_execute("SET koldstore.min_max_rows_per_file = 1")
            .await
            .context("allow small max_rows_per_file for test")?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 10,
                  min_flush_rows => 1,
                  max_rows_per_file => 100,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await
            .context("manage_table with low hot_row_limit")?;
        common::fence_async_mirror(&db.client).await?;

        let flushed = db.flush_table_with_force(&table.relation, true).await?;
        anyhow::ensure!(
            flushed >= 400,
            "expected cold archive before teardown probes, rows_flushed={flushed}"
        );
        common::assert_no_active_jobs(&db.client, &table.relation).await?;

        let hot = common::hot_row_count(&db.client, &table.relation).await?;
        anyhow::ensure!(
            hot <= 10,
            "expected hot prune to hot_row_limit after force flush, hot={hot}"
        );

        // --- Path A: fail-closed seen-key ERROR, then same-session follow-ups ---
        db.client
            .batch_execute("SET koldstore.max_merge_seen_keys = 50")
            .await
            .context("set low seen-key cap")?;

        let limited = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await;
        let err = limited
            .err()
            .context("count(*) must ERROR when seen-key cap is exceeded")?;
        let err_text = err
            .as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| err.to_string());
        if !(err_text.contains("exact primary-key identities")
            || err_text.contains("max_merge_seen_keys")
            || err_text.contains("retained too many"))
        {
            bail!("unexpected seen-key failure: {err_text}");
        }

        db.client
            .batch_execute("SELECT 1")
            .await
            .context("backend must stay alive after seen-key ERROR")?;

        let cold_pk: i64 = db
            .client
            .query_one(
                &format!("SELECT id FROM {} WHERE id = 1", table.relation),
                &[],
            )
            .await
            .context("point lookup after seen-key ERROR must not abort backend")?
            .get(0);
        anyhow::ensure!(cold_pk == 1);

        // --- Path B: unbounded merge count, then same-session follow-ups ---
        db.client
            .batch_execute("SET koldstore.max_merge_seen_keys = 0")
            .await
            .context("disable seen-key cap")?;

        let total: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*)::bigint FROM {}", table.relation),
                &[],
            )
            .await
            .context("unbounded hot+cold count(*)")?
            .get(0);
        anyhow::ensure!(
            total == 500,
            "unbounded count must see all managed rows, got {total}"
        );

        let followup: i64 = db
            .client
            .query_one(
                &format!("SELECT id FROM {} WHERE id = 42", table.relation),
                &[],
            )
            .await
            .context(
                "follow-up query after unbounded count must not abort backend \
                 (regression: double free / signal 6)",
            )?
            .get(0);
        anyhow::ensure!(followup == 42);

        db.client
            .batch_execute("SELECT 1")
            .await
            .context("backend must stay alive after unbounded count teardown")?;

        db.client
            .batch_execute("RESET koldstore.max_merge_seen_keys")
            .await
            .context("reset seen-key GUC")?;
    }
    Ok(())
}
