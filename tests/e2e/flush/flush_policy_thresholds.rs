//! E2E: hot_row_limit + min_flush_rows threshold behavior for flush_table.

#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[tokio::test]
async fn flush_honors_hot_row_limit_and_min_flush_rows_thresholds() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "flush_policy_thresholds").await?;
        let relation = format!("{}.policy_threshold_items", db.schema);

        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  body text NOT NULL
                );
                INSERT INTO {relation} (id, body)
                SELECT gs, 'row-' || gs::text
                FROM generate_series(1, 60) AS gs;
                "#
            ))
            .await?;

        // excess = 10 (< min_flush_rows=20) → non-force flush moves nothing.
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name         => $1::text::regclass,
                  storage            => $2,
                  hot_row_limit      => 50,
                  min_flush_rows     => 20,
                  max_rows_per_file  => 1000,
                  migration_order_by => 'id'
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;

        let below = flush_rows(&db, &relation, false).await?;
        assert_eq!(
            below, 0,
            "10 excess rows below min_flush_rows=20 must not flush"
        );
        assert_eq!(
            common::cold_segment_count(&db.client, &relation).await?,
            0,
            "no segments before threshold"
        );

        // Grow to 80 rows → excess=30 (>= 20) → flush 30.
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, body)
                SELECT gs, 'row-' || gs::text
                FROM generate_series(61, 80) AS gs;
                "#
            ))
            .await?;

        let flushed = flush_rows(&db, &relation, false).await?;
        assert_eq!(
            flushed, 30,
            "expected policy to flush excess=30 when above min_flush_rows"
        );
        assert!(
            common::cold_segment_count(&db.client, &relation).await? > 0,
            "threshold flush must publish segments"
        );

        let status = common::describe_table(&db.client, &relation).await?;
        assert!(
            status.hot_rows <= 50,
            "hot rows should be at/under hot_row_limit after flush, got {}",
            status.hot_rows
        );
        assert!(
            status.cold_row_count >= 30,
            "cold rows should include flushed excess, got {}",
            status.cold_row_count
        );

        // Below-limit force flush still drains remaining hot mirror rows.
        let force_flushed = flush_rows(&db, &relation, true).await?;
        assert!(
            force_flushed > 0,
            "force flush should drain remaining hot rows, got {force_flushed}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn pre_flush_force_reserves_below_hot_row_limit() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "pre_flush_force").await?;
        let table = db
            .create_indexed_items_table("pre_flush_force_items", 8)
            .await?;

        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => 100,
                  min_flush_rows => 1,
                  migration_order_by => 'id'
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;

        let reserved: i64 = db
            .client
            .query_one(
                "SELECT koldstore.pre_flush_table($1::text::regclass, force => true)",
                &[&table.relation],
            )
            .await?
            .get(0);
        assert_eq!(
            reserved, 1,
            "force pre_flush must reserve pending below limit"
        );

        // Non-force flush still refuses to drain below hot_row_limit.
        let non_force = flush_rows(&db, &table.relation, false).await?;
        assert_eq!(non_force, 0);

        let pending: i64 = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.pending p
                WHERE p.table_oid = $1::text::regclass::oid
                "#,
                &[&table.relation],
            )
            .await?
            .get(0);
        assert_eq!(pending, 1, "pending must remain after non-force flush");

        let force = flush_rows(&db, &table.relation, true).await?;
        assert_eq!(force, 8);
    }

    Ok(())
}

async fn flush_rows(db: &common::TestDb, relation: &str, force: bool) -> Result<i64> {
    let job_id: String = db
        .client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass, $2)::text",
            &[&relation, &force],
        )
        .await?
        .get(0);
    let flushed: i64 = db
        .client
        .query_one(
            "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        )
        .await?
        .get(0);
    Ok(flushed)
}
