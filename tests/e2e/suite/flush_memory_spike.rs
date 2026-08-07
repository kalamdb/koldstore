//! Peak RSS / context gates for flush and large merge-scan queries.
//!
//! Complements retained-growth sampling in `memory_leak.rs` by polling cluster
//! RSS while flush / large SELECTs run so mid-operation spikes cannot hide.

use std::time::Duration;

use anyhow::{bail, Result};

use crate::common;

fn assert_within_spike_budget(
    label: &str,
    peak: koldstore_memory::PeakAllocation,
    budget: common::memory::SpikeBudget,
) -> Result<()> {
    let rss_spike = peak.rss_spike_bytes();
    let ctx_spike = peak.context_spike_bytes();
    let rss_retained = u64::try_from(peak.rss_delta_bytes().max(0)).unwrap_or(0);
    let ctx_retained = u64::try_from(peak.context_delta_bytes().max(0)).unwrap_or(0);
    common::log(format!(
        "{label}: rss_spike={rss_spike} ctx_spike={ctx_spike} \
         rss_retained={rss_retained} ctx_retained={ctx_retained} \
         before_rss={} peak_rss={} after_rss={}",
        peak.before.rss_bytes, peak.peak.rss_bytes, peak.after.rss_bytes
    ));
    if rss_spike > budget.max_rss_spike_bytes
        || ctx_spike > budget.max_context_spike_bytes
        || rss_retained > budget.max_rss_retained_bytes
        || ctx_retained > budget.max_context_retained_bytes
    {
        bail!(
            "{label} exceeded spike budget: rss_spike={rss_spike}/{} \
             ctx_spike={ctx_spike}/{} rss_retained={rss_retained}/{} \
             ctx_retained={ctx_retained}/{}",
            budget.max_rss_spike_bytes,
            budget.max_context_spike_bytes,
            budget.max_rss_retained_bytes,
            budget.max_context_retained_bytes
        );
    }
    Ok(())
}

/// Flush must not unbounded-spike cluster RSS while Parquet encode runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_peak_rss_stays_within_budget() -> Result<()> {
    common::require_pgrx_server().await?;
    let budget = common::memory::spike_budget_from_env();
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "mem_flush_peak").await?;
        let rows = 4_000_i64;
        let table = db
            .create_indexed_items_table("flush_peak_items", rows)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror(&db.client).await?;

        let relation = table.relation.clone();
        let peak = common::memory::measure_peak_during(
            &db.client,
            db.target.port,
            Duration::from_millis(25),
            || async {
                let flushed = db.flush_table_with_force(&relation, true).await?;
                anyhow::ensure!(flushed > 0, "flush must archive rows");
                Ok(())
            },
        )
        .await?;
        assert_within_spike_budget("flush peak", peak, budget)?;
        common::assert_no_active_jobs(&db.client, &table.relation).await?;
    }
    Ok(())
}

/// Large hot+cold SELECT must stream without unbounded RSS / context spikes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_merge_scan_query_peak_memory_stays_within_budget() -> Result<()> {
    common::require_pgrx_server().await?;
    let budget = common::memory::spike_budget_from_env();
    let rows = common::memory::large_query_rows();
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "mem_large_q").await?;
        let table = db
            .create_indexed_items_table("large_query_items", rows)
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 500,
                  min_flush_rows => 1,
                  max_rows_per_file => 2000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;
        let flushed = db.flush_table_with_force(&table.relation, true).await?;
        anyhow::ensure!(flushed > 0);
        // Leave a hot tail so the merge path is exercised.
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, account_id, title, qty, category)
                SELECT
                  gs::bigint,
                  1,
                  'hot-' || gs::text,
                  1,
                  'hot'
                FROM generate_series({start}, {end}) AS gs;
                "#,
                relation = table.relation,
                start = rows + 1,
                end = rows + 200,
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let relation = table.relation.clone();
        let peak = common::memory::measure_peak_during(
            &db.client,
            db.target.port,
            Duration::from_millis(20),
            || async {
                let count: i64 = db
                    .client
                    .query_one(
                        &format!("SELECT count(*) FROM {relation} WHERE id >= 1"),
                        &[],
                    )
                    .await?
                    .get(0);
                anyhow::ensure!(
                    count == rows + 200,
                    "large query must return all rows, got {count}"
                );
                let page: Vec<i64> = db
                    .client
                    .query(
                        &format!("SELECT id FROM {relation} WHERE id >= $1 ORDER BY id LIMIT $2"),
                        &[&1_i64, &100_i64],
                    )
                    .await?
                    .into_iter()
                    .map(|row| row.get(0))
                    .collect();
                anyhow::ensure!(
                    page == (1..=100).collect::<Vec<_>>(),
                    "ordered LIMIT page must be 1..=100, got {page:?}"
                );
                Ok(())
            },
        )
        .await?;
        assert_within_spike_budget("large merge-scan query", peak, budget)?;
    }
    Ok(())
}
