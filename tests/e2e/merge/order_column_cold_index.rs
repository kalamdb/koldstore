//! E2E coverage for Storekey cold segment index pruning via order column.

use crate::common;

use anyhow::Result;

/// Flush two time-disjoint waves, then prove SQL segment-index pruning keeps
/// only the overlapping cold segments for an order-column range predicate.
#[tokio::test]
async fn order_column_range_uses_cold_segment_index() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "order_col_index").await?;
        let relation = db.relation("order_events");
        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  event_time timestamptz NOT NULL,
                  payload text NOT NULL
                );
                "#
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => NULL,
                  migration_order_by => 'id',
                  mirror_capture_mode => 'strict',
                  segment_order_column => 'event_time'
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        db.client
            .execute(
                "SELECT koldstore.set_table_auto_flush($1::text::regclass, false)",
                &[&relation],
            )
            .await?;

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, event_time, payload)
                SELECT gs,
                       timestamptz '2024-01-01 00:00:00+00' + (gs || ' minutes')::interval,
                       'early'
                FROM generate_series(1, 1200) AS gs;
                "#
            ))
            .await?;
        let flushed_early = force_flush(&db, &relation).await?;
        anyhow::ensure!(flushed_early > 0, "early wave flushed no rows");

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, event_time, payload)
                SELECT gs,
                       timestamptz '2025-01-01 00:00:00+00' + ((gs - 2000) || ' minutes')::interval,
                       'late'
                FROM generate_series(2001, 3200) AS gs;
                "#
            ))
            .await?;
        let flushed_late = force_flush(&db, &relation).await?;
        anyhow::ensure!(flushed_late > 0, "late wave flushed no rows");

        let index_rows: i64 = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.cold_segment_index
                WHERE table_oid = $1::text::regclass::oid
                  AND column_id = (
                      SELECT attnum
                      FROM pg_catalog.pg_attribute
                      WHERE attrelid = $1::text::regclass
                        AND attname = 'event_time'
                        AND NOT attisdropped
                  )
                  AND codec_version = 1
                  AND min_value IS NOT NULL
                  AND max_value IS NOT NULL
                "#,
                &[&relation],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            index_rows >= 2,
            "expected Storekey index rows for event_time, got {index_rows}"
        );

        let mirror_has_order_key: bool = db
            .client
            .query_one(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.columns
                  WHERE table_schema = 'koldstore'
                    AND table_name = $1
                    AND column_name = 'order_key'
                )
                "#,
                &[&format!(
                    "{}__cl",
                    relation.rsplit('.').next().unwrap_or(relation.as_str())
                )],
            )
            .await?
            .get(0);
        anyhow::ensure!(mirror_has_order_key, "mirror must include order_key");

        let late_only = format!(
            "SELECT count(*) FROM {relation} \
             WHERE event_time >= timestamptz '2025-01-01 00:00:00+00'"
        );
        let visible: i64 = db.client.query_one(&late_only, &[]).await?.get(0);
        anyhow::ensure!(
            visible == 1200,
            "late-range count must stay correct, got {visible}"
        );

        let plan = common::explain_analyze(&db.client, &late_only).await?;
        common::assertions::assert_kold_merge_scan_explain(&plan)?;
        anyhow::ensure!(
            plan.contains("Segment Index Source: postgres (koldstore.cold_segment_index)"),
            "expected SQL segment-index source, got:\n{plan}"
        );
        anyhow::ensure!(
            plan.contains("Segment Index Lookup Shape:"),
            "expected lookup shape in EXPLAIN, got:\n{plan}"
        );
        anyhow::ensure!(
            plan.contains("Order Column ID:"),
            "expected order column id in EXPLAIN, got:\n{plan}"
        );

        let opened = explain_counter(&plan, "Parquet Segments Opened")?;
        let considered = explain_counter(&plan, "Candidate Segments")?;
        anyhow::ensure!(
            opened < considered,
            "order-column range should prune at least one cold segment; opened={opened} considered={considered}\n{plan}"
        );

        let renamed = format!(
            "SELECT count(*) FROM {relation} \
             WHERE event_time >= timestamptz '2025-01-01 00:00:00+00'"
        );
        db.client
            .batch_execute(&format!(
                "ALTER TABLE {relation} RENAME COLUMN event_time TO occurred_at;"
            ))
            .await?;
        let after_rename: i64 = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*) FROM {relation} \
                     WHERE occurred_at >= timestamptz '2025-01-01 00:00:00+00'"
                ),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            after_rename == 1200,
            "rename must keep order-column identity, got {after_rename}; probe was {renamed}"
        );
    }
    Ok(())
}

fn explain_counter(plan: &str, label: &str) -> Result<usize> {
    let needle = format!("{label}: ");
    let line = plan
        .lines()
        .find(|line| line.contains(&needle))
        .ok_or_else(|| anyhow::anyhow!("missing EXPLAIN counter {label}"))?;
    let value = line
        .rsplit(':')
        .next()
        .ok_or_else(|| anyhow::anyhow!("malformed EXPLAIN counter {label}"))?
        .trim();
    Ok(value.parse()?)
}

async fn force_flush(db: &common::TestDb, relation: &str) -> Result<i64> {
    let row = db
        .client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass, true)::text",
            &[&relation],
        )
        .await?;
    let job_id: String = row.get(0);
    let progress = db
        .client
        .query_one(
            "SELECT rows_flushed, status, error_trace FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        )
        .await?;
    let rows_flushed: i64 = progress.get(0);
    let status: String = progress.get(1);
    let error_trace: Option<String> = progress.get(2);
    anyhow::ensure!(
        status == "completed",
        "force flush ended as {status}: {}",
        error_trace.unwrap_or_default()
    );
    Ok(rows_flushed)
}
