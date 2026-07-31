//! E2E coverage for Storekey cold segment index pruning via order column.

use crate::common;

use anyhow::Result;

/// Flush two time-disjoint waves, then prove SQL segment-index pruning for
/// lower-only, upper-only, and bounded predicates independently.
#[tokio::test]
async fn order_column_range_shapes_use_cold_segment_index() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "order_col_shapes").await?;
        let relation = setup_order_table(&db, "order_events_shapes").await?;

        let late_only = format!(
            "SELECT count(*) FROM {relation} \
             WHERE event_time >= timestamptz '2025-01-01 00:00:00+00'"
        );
        assert_shape_plan(&db, &relation, &late_only, "lower_bound", 1200).await?;

        let early_only = format!(
            "SELECT count(*) FROM {relation} \
             WHERE event_time <= timestamptz '2024-01-01 12:00:00+00'"
        );
        assert_shape_plan(&db, &relation, &early_only, "upper_bound", 720).await?;

        let bounded = format!(
            "SELECT count(*) FROM {relation} \
             WHERE event_time >= timestamptz '2025-01-01 00:00:00+00' \
               AND event_time <= timestamptz '2025-01-01 12:00:00+00'"
        );
        assert_shape_plan(&db, &relation, &bounded, "bounded_range", 720).await?;

        let between = format!(
            "SELECT count(*) FROM {relation} \
             WHERE event_time BETWEEN timestamptz '2025-01-01 00:00:00+00' \
               AND timestamptz '2025-01-01 12:00:00+00'"
        );
        assert_shape_plan(&db, &relation, &between, "bounded_range", 720).await?;

        let candidate_plan = explain_candidate_index_sql(&db, &relation, "max").await?;
        anyhow::ensure!(
            candidate_plan.contains("cold_segment_index")
                && (candidate_plan.contains("Index") || candidate_plan.contains("Seq Scan")),
            "lower-bound candidate SQL should use cold_segment_index access, got:\n{candidate_plan}"
        );
        anyhow::ensure!(
            !candidate_plan.to_lowercase().contains("force"),
            "candidate SQL must not force an index:\n{candidate_plan}"
        );
        let upper_plan = explain_candidate_index_sql(&db, &relation, "min").await?;
        anyhow::ensure!(
            upper_plan.contains("cold_segment_index")
                && (upper_plan.contains("Index") || upper_plan.contains("Seq Scan")),
            "upper-bound candidate SQL should use cold_segment_index access, got:\n{upper_plan}"
        );

        let unsupported_or = format!(
            "SELECT count(*) FROM {relation} \
             WHERE event_time >= timestamptz '2025-01-01 00:00:00+00' \
                OR payload = 'never'"
        );
        let or_count: i64 = db.client.query_one(&unsupported_or, &[]).await?.get(0);
        anyhow::ensure!(
            or_count == 1200,
            "OR fallback must stay correct, got {or_count}"
        );
        let or_plan = common::explain_analyze(&db.client, &unsupported_or).await?;
        anyhow::ensure!(
            or_plan.contains("Segment Index Lookup Shape: all_active")
                || !or_plan.contains("Segment Index Lookup Shape: lower_bound"),
            "unsupported OR must not use a one-sided index prune:\n{or_plan}"
        );

        db.client
            .batch_execute(&format!(
                "ALTER TABLE {relation} RENAME COLUMN event_time TO occurred_at;"
            ))
            .await?;
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, occurred_at, payload)
                VALUES (99999, timestamptz '2025-06-01 00:00:00+00', 'after-rename');
                "#
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;
        let renamed_row: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*) FROM {relation} WHERE id = 99999"),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            renamed_row == 1,
            "DML after order-column rename must be visible, got {renamed_row}"
        );
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
            after_rename == 1201,
            "rename must keep order-column identity and accept new DML, got {after_rename}"
        );
    }
    Ok(())
}

/// Async capture must persist and retain encoded mirror `order_key` across delete.
#[tokio::test]
async fn async_order_column_retains_mirror_order_key() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "order_col_async").await?;
        let relation = db.relation("async_order_events");
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
                VALUES
                  (1, timestamptz '2024-06-01 00:00:00+00', 'a'),
                  (2, timestamptz '2024-06-02 00:00:00+00', 'b');
                "#
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let mirror = format!(
            "koldstore.{}__cl",
            relation.rsplit('.').next().unwrap_or(relation.as_str())
        );
        let before: Vec<u8> = db
            .client
            .query_one(&format!("SELECT order_key FROM {mirror} WHERE id = 1"), &[])
            .await?
            .get(0);
        anyhow::ensure!(!before.is_empty(), "async insert must encode order_key");

        db.client
            .batch_execute(&format!("DELETE FROM {relation} WHERE id = 1;"))
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let after: Vec<u8> = db
            .client
            .query_one(&format!("SELECT order_key FROM {mirror} WHERE id = 1"), &[])
            .await?
            .get(0);
        anyhow::ensure!(
            after == before,
            "delete must retain the encoded order_key on the mirror tombstone"
        );

        let reject = db
            .client
            .execute(
                &format!(
                    "UPDATE {relation} SET event_time = timestamptz '2030-01-01 00:00:00+00' WHERE id = 2"
                ),
                &[],
            )
            .await;
        anyhow::ensure!(
            reject.is_err(),
            "order-column mutation must be rejected under async capture"
        );
    }
    Ok(())
}

async fn assert_shape_plan(
    db: &common::TestDb,
    _relation: &str,
    sql: &str,
    expected_shape: &str,
    expected_count: i64,
) -> Result<()> {
    let visible: i64 = db.client.query_one(sql, &[]).await?.get(0);
    anyhow::ensure!(
        visible == expected_count,
        "count mismatch for {expected_shape}: got {visible}, want {expected_count}"
    );
    let plan = common::explain_analyze(&db.client, sql).await?;
    common::assertions::assert_kold_merge_scan_explain(&plan)?;
    anyhow::ensure!(
        plan.contains("Segment Index Source: postgres (koldstore.cold_segment_index)"),
        "expected SQL segment-index source, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.contains(&format!("Segment Index Lookup Shape: {expected_shape}")),
        "expected lookup shape {expected_shape}, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.contains("Order Column ID:"),
        "expected order column id in EXPLAIN, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.contains("Order Column: event_time") || plan.contains("Order Column: occurred_at"),
        "expected order column name in EXPLAIN, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.contains("Segment Index Preferred Access:"),
        "expected Segment Index Preferred Access in EXPLAIN, got:\n{plan}"
    );
    match expected_shape {
        "lower_bound" => anyhow::ensure!(
            plan.contains("Segment Index Preferred Access: max_idx"),
            "lower-bound should prefer max_idx:\n{plan}"
        ),
        "upper_bound" => anyhow::ensure!(
            plan.contains("Segment Index Preferred Access: min_idx"),
            "upper-bound should prefer min_idx:\n{plan}"
        ),
        "bounded_range" => anyhow::ensure!(
            plan.contains("Segment Index Preferred Access: bitmap_and_or_single"),
            "bounded range should allow planner choice:\n{plan}"
        ),
        _ => {}
    }
    anyhow::ensure!(
        plan.contains("Segments Returned by Segment Index:")
            || plan.contains("Segment Index Candidates:"),
        "expected segments-returned counter, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.contains("Candidate Segments:"),
        "expected candidate segments counter, got:\n{plan}"
    );

    let opened = explain_counter(&plan, "Parquet Segments Opened")?;
    let considered = explain_counter(&plan, "Candidate Segments")?;
    anyhow::ensure!(
        opened < considered,
        "order-column range should prune at least one cold segment; opened={opened} considered={considered}\n{plan}"
    );
    Ok(())
}

async fn explain_candidate_index_sql(
    db: &common::TestDb,
    relation: &str,
    side: &str,
) -> Result<String> {
    let predicate = match side {
        "max" => "csi.max_value >= E'\\\\x00'::bytea",
        "min" => "csi.min_value <= E'\\\\xff'::bytea",
        other => anyhow::bail!("unknown candidate side {other}"),
    };
    let rows = db
        .client
        .query(
            &format!(
                r#"
                EXPLAIN (FORMAT TEXT)
                SELECT csi.segment_id
                FROM koldstore.cold_segment_index csi
                WHERE csi.table_oid = $1::text::regclass::oid
                  AND csi.scope_key = ''
                  AND csi.column_id = (
                        SELECT attnum FROM pg_catalog.pg_attribute
                        WHERE attrelid = $1::text::regclass
                          AND attname = 'event_time'
                          AND NOT attisdropped
                      )
                  AND csi.codec_version = 1
                  AND {predicate}
                "#
            ),
            &[&relation],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

async fn setup_order_table(db: &common::TestDb, table: &str) -> Result<String> {
    let relation = db.relation(table);
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
              max_rows_per_file => 200,
              migration_order_by => 'id',
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
    let flushed_early = force_flush(db, &relation).await?;
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
    let flushed_late = force_flush(db, &relation).await?;
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
    Ok(relation)
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
