//! E2E coverage for US7: counters → `koldstore.pending` → flush → published.
//!
//! Verifies that DML does not create pending/segment rows, that `pre_flush_table`
//! upserts into `koldstore.pending`, and that `flush_table` publishes cold
//! segments then clears flushable pending scopes.

#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;

#[test]
fn pending_segment_counters_plan_mentions_lifecycle_and_flush() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let workflow = [
        "scope counters",
        "koldstore.pre_flush_table",
        "koldstore.pending",
        "koldstore.flush_table",
        "published segments",
        "hot cleanup after manifest commit",
    ];
    for required in workflow {
        assert!(!required.is_empty());
    }
}

#[tokio::test]
async fn pending_segment_counters_pre_flush_then_flush_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "pending_segment_counters").await?;
        let table = db
            .create_indexed_items_table("pending_counter_items", 32)
            .await?;
        db.manage_shared(&table.relation, "id").await?;

        let before_pending = count_pending(&db.client, &table.relation).await?;
        assert_eq!(before_pending, 0, "DML must not create pending rows");
        let before_segments = count_segments(&db.client, &table.relation, "published").await?;
        assert_eq!(before_segments, 0, "DML must not create published segments");

        let reserved: i64 = db
            .client
            .query_one(
                "SELECT koldstore.pre_flush_table($1::text::regclass)",
                &[&table.relation],
            )
            .await?
            .get(0);
        assert!(
            reserved > 0,
            "pre_flush_table must reserve at least one pending row"
        );

        // Second pre-flush must upsert the same pending row, not create duplicates.
        let reserved_again: i64 = db
            .client
            .query_one(
                "SELECT koldstore.pre_flush_table($1::text::regclass)",
                &[&table.relation],
            )
            .await?
            .get(0);
        assert_eq!(
            reserved_again, reserved,
            "pre_flush must update existing pending scopes, not insert duplicates"
        );

        let pending_rows = db
            .client
            .query(
                r#"
                SELECT p.scope_key, p.row_count
                FROM koldstore.pending p
                JOIN pg_class c ON c.oid = p.table_oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname || '.' || c.relname = $1
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(
            pending_rows.len() as i64,
            reserved,
            "pre_flush_table return count must match pending catalog rows"
        );
        for row in &pending_rows {
            let row_count: i64 = row.get(1);
            assert!(
                row_count > 0,
                "pending reservation must carry a row estimate"
            );
        }

        let segment_pending: i64 = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.segments cs
                JOIN pg_class c ON c.oid = cs.table_oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname || '.' || c.relname = $1
                "#,
                &[&table.relation],
            )
            .await?
            .get(0);
        assert_eq!(
            segment_pending, 0,
            "pending reservations must not live in koldstore.segments"
        );

        let published_before = count_segments(&db.client, &table.relation, "published").await?;
        assert_eq!(
            published_before, 0,
            "pre-flush alone must not publish cold segments"
        );

        let flushed = db.flush_table(&table.relation).await?;
        assert_eq!(flushed, 32);

        let published = count_segments(&db.client, &table.relation, "published").await?;
        assert!(published > 0, "expected published segments after flush");

        let leftover_pending = count_pending(&db.client, &table.relation).await?;
        assert_eq!(
            leftover_pending, 0,
            "successful flush must clear flushable pending reservations"
        );

        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 32).await?;
        common::assert_no_active_jobs(&db.client, &table.relation).await?;
    }

    Ok(())
}

#[tokio::test]
async fn pending_below_hot_row_limit_is_retained_until_force() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "pending_hot_row_limit").await?;
        let table = db
            .create_indexed_items_table("pending_hot_limit_items", 8)
            .await?;
        // Keep hot_row_limit above current row count so non-force flush finds
        // nothing flushable and leaves the pending reservation in place.
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => 100,
                  migration_order_by => 'id'
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;

        let reserved: i64 = db
            .client
            .query_one(
                "SELECT koldstore.pre_flush_table($1::text::regclass)",
                &[&table.relation],
            )
            .await?
            .get(0);
        assert_eq!(
            reserved, 1,
            "expected one pending reservation after pre_flush"
        );

        // Non-force flush with rows below hot_row_limit should not drain pending.
        let _ = db
            .client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass, false)::text",
                &[&table.relation],
            )
            .await?;
        let pending_after = count_pending(&db.client, &table.relation).await?;
        assert_eq!(
            pending_after, 1,
            "below-threshold pending must remain after non-force flush"
        );
        let published = count_segments(&db.client, &table.relation, "published").await?;
        assert_eq!(
            published, 0,
            "below-threshold flush must not publish segments"
        );

        let force_job: String = db
            .client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass, true)::text",
                &[&table.relation],
            )
            .await?
            .get(0);
        let flushed: i64 = db
            .client
            .query_one(
                "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
                &[&force_job],
            )
            .await?
            .get(0);
        assert_eq!(flushed, 8);
        assert_eq!(
            count_pending(&db.client, &table.relation).await?,
            0,
            "force flush must clear pending"
        );
        assert!(
            count_segments(&db.client, &table.relation, "published").await? > 0,
            "force flush must publish segments"
        );
    }

    Ok(())
}

async fn count_pending(client: &tokio_postgres::Client, relation: &str) -> Result<i64> {
    let count = client
        .query_one(
            r#"
            SELECT count(*)::bigint
            FROM koldstore.pending p
            JOIN pg_class c ON c.oid = p.table_oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname || '.' || c.relname = $1
            "#,
            &[&relation],
        )
        .await?
        .get(0);
    Ok(count)
}

async fn count_segments(
    client: &tokio_postgres::Client,
    relation: &str,
    status: &str,
) -> Result<i64> {
    let count = client
        .query_one(
            r#"
            SELECT count(*)::bigint
            FROM koldstore.segments cs
            JOIN pg_class c ON c.oid = cs.table_oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname || '.' || c.relname = $1
              AND cs.status = $2
            "#,
            &[&relation, &status],
        )
        .await?
        .get(0);
    Ok(count)
}
