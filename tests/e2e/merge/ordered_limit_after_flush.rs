//! Regression: ordered managed SELECTs must not omit cold rows after flush.
//!
//! After multi-wave flush the hot heap is empty while cold Parquet holds the
//! data. `ORDER BY … LIMIT` used to prefer leftover parallel IndexScan /
//! Gather Merge paths on the heap, returning zero rows while `count(*)` on
//! the same filter still saw cold via KoldMergeScan.

use anyhow::{Context, Result};

use crate::common;
use crate::flush::harness::{barrier_unlock, pause_flush_at};

#[tokio::test]
async fn ordered_limit_after_multi_wave_flush_returns_cold_rows() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_ordered_limit").await?;
        // Seed in waves so cold grows across multiple segments (high CustomScan
        // cost makes a leaked heap-only ordered path look attractive).
        let table = db
            .create_indexed_items_table("ordered_items", 2_000)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        db.client
            .execute(
                "SELECT koldstore.set_table_auto_flush($1::text::regclass, false)",
                &[&table.relation],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        for wave in 0..3 {
            if wave > 0 {
                let start = wave * 2_000 + 1;
                let end = (wave + 1) * 2_000;
                db.client
                    .batch_execute(&format!(
                        r#"
                        INSERT INTO {relation} (id, account_id, title, qty, category)
                        SELECT
                          gs::bigint,
                          (gs % 17)::bigint,
                          'item-' || lpad(gs::text, 6, '0'),
                          (gs % 100)::integer,
                          CASE WHEN gs % 2 = 0 THEN 'even' ELSE 'odd' END
                        FROM generate_series({start}, {end}) AS gs;
                        "#,
                        relation = table.relation
                    ))
                    .await?;
                common::fence_async_mirror(&db.client).await?;
            }
            let flushed = db.flush_table(&table.relation).await?;
            anyhow::ensure!(flushed > 0, "wave {wave} flush archived no rows");
            common::assert_no_active_jobs(&db.client, &table.relation).await?;
        }

        let hot = common::hot_row_count(&db.client, &table.relation).await?;
        anyhow::ensure!(hot == 0, "expected hot prune after flush waves, hot={hot}");

        let visible: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get(0);
        anyhow::ensure!(
            visible == 6_000,
            "managed count(*) must see all rows after flush, got {visible}"
        );

        // PostgreSQL's Limit node must be able to stop KoldMergeScan before it
        // opens every eligible segment. This is the common filtered pagination
        // shape; ORDER BY is tested separately below because PostgreSQL must
        // consume the full unordered child before sorting it.
        let bounded_plan = common::explain_analyze(
            &db.client,
            &format!("SELECT id FROM {} WHERE id >= 1 LIMIT 100", table.relation),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&bounded_plan)?;
        let candidates = explain_counter(&bounded_plan, "Candidate Segments")?;
        let opened = explain_counter(&bounded_plan, "Parquet Segments Opened")?;
        anyhow::ensure!(
            candidates >= 3,
            "expected all three flushed segments to remain candidates:\n{bounded_plan}"
        );
        anyhow::ensure!(
            opened == 1,
            "filtered LIMIT should stop after one disjoint segment, opened={opened}:\n{bounded_plan}"
        );

        // Cheap parallel setup so leftover partial IndexScan paths would produce
        // Gather Merge if `partial_pathlist` were not cleared.
        db.client
            .batch_execute(
                "SET max_parallel_workers_per_gather = 2;\
                 SET parallel_setup_cost = 0;\
                 SET parallel_tuple_cost = 0;\
                 SET min_parallel_table_scan_size = 0;\
                 SET min_parallel_index_scan_size = 0;",
            )
            .await?;

        let plan = common::explain(
            &db.client,
            &format!(
                "SELECT id FROM {} WHERE id >= 1 ORDER BY id ASC LIMIT 5",
                table.relation
            ),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan)?;
        anyhow::ensure!(
            !plan.contains("Gather Merge"),
            "ordered managed SELECT must not use Gather Merge over heap-only paths:\n{plan}"
        );
        anyhow::ensure!(
            !bare_heap_index_scan_without_merge(&plan),
            "ordered managed SELECT must not use a bare heap Index Scan:\n{plan}"
        );

        let rows = db
            .client
            .query(
                &format!(
                    "SELECT id FROM {} WHERE id >= $1 ORDER BY id ASC LIMIT $2",
                    table.relation
                ),
                &[&1_i64, &5_i64],
            )
            .await
            .context("parameterized ORDER BY LIMIT after flush")?;
        anyhow::ensure!(
            rows.len() == 5,
            "ORDER BY id LIMIT 5 must return 5 cold-backed rows, got {}",
            rows.len()
        );
        let ids: Vec<i64> = rows.iter().map(|row| row.get(0)).collect();
        anyhow::ensure!(
            ids == vec![1, 2, 3, 4, 5],
            "expected first five ids, got {ids:?}"
        );

        // Non-selected ORDER BY column (resjunk) must also see cold rows.
        let by_title: i64 = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*) FROM (
                       SELECT id FROM {} WHERE title >= 'item-' ORDER BY title ASC LIMIT 10
                     ) s",
                    table.relation
                ),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            by_title == 10,
            "ORDER BY non-selected column LIMIT must return rows, got {by_title}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn ordered_limit_user_scope_after_flush_uses_merge_scan() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_ordered_scope").await?;
        let relation = db.relation("ordered_notes");
        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  user_id text NOT NULL,
                  title text NOT NULL,
                  body text NOT NULL
                );
                CREATE INDEX ordered_notes_user_id_idx ON {relation} (user_id, id);
                INSERT INTO {relation} (id, user_id, title, body)
                SELECT gs, 'user-a', 'note-' || gs, 'body-' || gs
                FROM generate_series(1, 2500) AS gs;
                ANALYZE {relation};
                "#
            ))
            .await?;
        db.manage_user_scoped(&relation, "user_id").await?;
        db.client
            .execute(
                "SELECT koldstore.set_table_auto_flush($1::text::regclass, false)",
                &[&relation],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;

        for _ in 0..2 {
            let flushed = db.flush_table(&relation).await?;
            if flushed == 0 {
                break;
            }
            common::assert_no_active_jobs(&db.client, &relation).await?;
        }

        db.client
            .batch_execute("SET koldstore.user_id = 'user-a'")
            .await?;
        let plan = common::explain(
            &db.client,
            &format!("SELECT id FROM {relation} WHERE user_id = 'user-a' ORDER BY id ASC LIMIT 3"),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&plan)?;

        let rows = db
            .client
            .query(
                &format!("SELECT id FROM {relation} WHERE user_id = $1 ORDER BY id ASC LIMIT $2"),
                &[&"user-a", &3_i64],
            )
            .await?;
        anyhow::ensure!(
            rows.len() == 3,
            "scoped ORDER BY LIMIT must return 3 rows after flush, got {}",
            rows.len()
        );
        let first: i64 = rows[0].get(0);
        anyhow::ensure!(first == 1, "lowest id should be 1, got {first}");
    }
    Ok(())
}

/// While flush is paused mid-publish, ordered LIMIT must still return every
/// visible row (hot + already-published cold + mirror), never an empty result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_limit_during_flush_sees_all_rows() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_ordered_midflush").await?;
        let table = db.create_indexed_items_table("midflush_items", 400).await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror(&db.client).await?;

        // Publish a first cold generation so queries must merge hot+cold.
        assert!(db.flush_table(&table.relation).await? > 0);
        common::assert_no_active_jobs(&db.client, &table.relation).await?;

        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, account_id, title, qty, category)
                SELECT
                  gs::bigint,
                  (gs % 17)::bigint,
                  'item-' || lpad(gs::text, 6, '0'),
                  (gs % 100)::integer,
                  CASE WHEN gs % 2 = 0 THEN 'even' ELSE 'odd' END
                FROM generate_series(401, 800) AS gs;
                "#,
                relation = table.relation
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let (coordinator, flush_handle) =
            pause_flush_at(&db, &table.relation, "wait:before_manifest_publish").await?;

        // Flush is holding the barrier: every seeded row must still be queryable.
        let mid_count: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get(0);
        anyhow::ensure!(
            mid_count == 800,
            "count(*) during flush must see all rows, got {mid_count}"
        );

        let mid_plan = common::explain(
            &db.client,
            &format!(
                "SELECT id FROM {} WHERE id >= 1 ORDER BY id ASC LIMIT 5",
                table.relation
            ),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&mid_plan)?;

        let mid_rows = db
            .client
            .query(
                &format!(
                    "SELECT id FROM {} WHERE id >= $1 ORDER BY id ASC LIMIT $2",
                    table.relation
                ),
                &[&1_i64, &5_i64],
            )
            .await
            .context("ORDER BY LIMIT during flush")?;
        anyhow::ensure!(
            mid_rows.len() == 5,
            "ORDER BY LIMIT during flush must return rows, got {}",
            mid_rows.len()
        );
        let mid_ids: Vec<i64> = mid_rows.iter().map(|row| row.get(0)).collect();
        anyhow::ensure!(
            mid_ids == vec![1, 2, 3, 4, 5],
            "ORDER BY LIMIT during flush must return lowest ids, got {mid_ids:?}"
        );

        barrier_unlock(&coordinator).await?;
        flush_handle.await??;
        common::assert_no_active_jobs(&db.client, &table.relation).await?;

        let after: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await?
            .get(0);
        anyhow::ensure!(
            after == 800,
            "post-flush count must remain 800, got {after}"
        );
    }
    Ok(())
}

fn bare_heap_index_scan_without_merge(plan: &str) -> bool {
    let has_index = plan.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed.starts_with("Index Scan") || trimmed.starts_with("Index Only Scan")
    });
    has_index && !plan.contains("Custom Scan (KoldMergeScan)")
}

/// Late materialization: compete opens for sort, body opens only when cold emits.
#[tokio::test]
async fn ordered_limit_late_materialization_stages() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "merge_late_mat").await?;
        let relation = db.relation("late_mat_items");
        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  title text NOT NULL,
                  body text NOT NULL
                );
                INSERT INTO {relation} (id, title, body)
                SELECT gs, 't-' || gs, repeat('x', 64) || gs::text
                FROM generate_series(1, 40) AS gs;
                "#
            ))
            .await?;
        db.manage_shared(&relation, "id").await?;
        db.client
            .execute(
                "SELECT koldstore.set_table_auto_flush($1::text::regclass, false)",
                &[&relation],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;
        anyhow::ensure!(db.flush_table(&relation).await? > 0);
        common::assert_no_active_jobs(&db.client, &relation).await?;

        // Flush prune frees heap PK slots; re-insert newer hot versions of the
        // lowest ids so ASC LIMIT can stop on hot after compete expands cold.
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, title, body)
                SELECT gs, 'hot-t-' || gs, 'hot-' || gs
                FROM generate_series(1, 8) AS gs;
                "#
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let hot_only = common::explain_analyze(
            &db.client,
            &format!("SELECT id, title, body FROM {relation} ORDER BY id ASC LIMIT 3"),
        )
        .await?;
        common::assert_kold_merge_scan_explain(&hot_only)?;
        anyhow::ensure!(
            hot_only.contains("Strategy: Ordered Progressive"),
            "expected Ordered Progressive:\n{hot_only}"
        );
        let compete = explain_counter(&hot_only, "Cold Compete Opens")?;
        let body = explain_counter(&hot_only, "Cold Body Opens")?;
        anyhow::ensure!(
            compete >= 1,
            "hot/cold overlap must compete before LIMIT:\n{hot_only}"
        );
        anyhow::ensure!(
            body == 0,
            "hot-only LIMIT after compete must skip body hydrate:\n{hot_only}"
        );
        anyhow::ensure!(
            hot_only.contains("Cold Body Columns:") && hot_only.contains("body"),
            "wide projection must advertise body columns:\n{hot_only}"
        );

        let hot_bodies: Vec<String> = db
            .client
            .query(
                &format!("SELECT body FROM {relation} ORDER BY id ASC LIMIT 3"),
                &[],
            )
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect();
        anyhow::ensure!(
            hot_bodies
                == vec![
                    "hot-1".to_string(),
                    "hot-2".to_string(),
                    "hot-3".to_string()
                ],
            "expected hot bodies, got {hot_bodies:?}"
        );

        // Cold outranks hot for DESC: body hydrate must run.
        let cold_emit = common::explain_analyze(
            &db.client,
            &format!("SELECT id, title, body FROM {relation} ORDER BY id DESC LIMIT 5"),
        )
        .await?;
        let compete_cold = explain_counter(&cold_emit, "Cold Compete Opens")?;
        let body_cold = explain_counter(&cold_emit, "Cold Body Opens")?;
        anyhow::ensure!(
            compete_cold >= 1 && body_cold >= 1,
            "cold emit must compete and hydrate body:\n{cold_emit}"
        );

        // Narrow SELECT fails open to Full (no compete/body split).
        let narrow = common::explain_analyze(
            &db.client,
            &format!("SELECT id FROM {relation} ORDER BY id DESC LIMIT 5"),
        )
        .await?;
        anyhow::ensure!(
            explain_counter(&narrow, "Cold Compete Opens")? == 0
                && explain_counter(&narrow, "Cold Body Opens")? == 0
                && explain_counter(&narrow, "Parquet Segments Opened")? >= 1,
            "narrow SELECT must fail-open to Full opens:\n{narrow}"
        );
        anyhow::ensure!(
            !narrow.contains("Cold Body Columns:"),
            "narrow fail-open must not list body columns:\n{narrow}"
        );
    }
    Ok(())
}

fn explain_counter(plan: &str, label: &str) -> Result<usize> {
    plan.lines()
        .find_map(|line| {
            line.trim_start()
                .strip_prefix(label)
                .and_then(|value| value.strip_prefix(':'))
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .with_context(|| format!("missing `{label}` counter in EXPLAIN:\n{plan}"))
}
