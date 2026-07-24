use crate::common;

use anyhow::Result;
use koldstore::merge_scan::exec::{begin_merge_scan_with_plan, ColdAvailability};
use koldstore::merge_scan::plan::{MergeScanPlan, SegmentHint};
use koldstore_common::{ScopeKey, SeqId};

#[test]
fn user_scope_cold_pruning_filters_segments_before_stream_open() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let mut plan = MergeScanPlan::new(42, vec!["id".to_string()]);
    plan.scope_key = Some(ScopeKey::new("user-a").unwrap());
    plan.segment_hints = vec![
        SegmentHint {
            segment_id: "segment-a".to_string(),
            scope_key: Some(ScopeKey::new("user-a").unwrap()),
            object_path: "app/notes/user-a/batch-1.parquet".to_string(),
            selected_row_groups: vec![0],
            min_seq: SeqId::new(1).unwrap(),
            max_seq: SeqId::new(2).unwrap(),
        },
        SegmentHint {
            segment_id: "segment-b".to_string(),
            scope_key: Some(ScopeKey::new("user-b").unwrap()),
            object_path: "app/notes/user-b/batch-1.parquet".to_string(),
            selected_row_groups: vec![1],
            min_seq: SeqId::new(1).unwrap(),
            max_seq: SeqId::new(2).unwrap(),
        },
    ];

    let state = begin_merge_scan_with_plan(&plan, ColdAvailability::Available).unwrap();

    assert_eq!(
        state.visible_segments,
        vec!["app/notes/user-a/batch-1.parquet"]
    );
    assert_eq!(state.selected_row_groups, vec![0]);
    assert_eq!(state.resources.object_store_handles, 1);
}

/// Scope equality uses `koldstore.cold_segment_stats` min/max on the shared
/// manifest (`scope_key = ''`) to skip other tenants' Parquet files.
///
/// Later each scope_id will own its own manifest.json + folder; listing will
/// filter by `scope_key` first and min/max remains a secondary prune.
#[tokio::test]
async fn scope_column_equality_prunes_cold_segments_via_catalog_stats() -> Result<()> {
    common::require_pgrx_server().await?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "scope_stats_prune").await?;
        let relation = db.relation("scope_stats_notes");
        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  user_id text NOT NULL,
                  title text NOT NULL,
                  body text NOT NULL
                );
                CREATE INDEX scope_stats_notes_user_id_idx ON {relation} (user_id, id);
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

        // Wave A: only user-a → segment(s) whose user_id stats are user-a.
        db.client
            .batch_execute("SET koldstore.user_id = 'user-a'")
            .await?;
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, user_id, title, body)
                SELECT gs, 'user-a', 'a-' || gs, 'body-a'
                FROM generate_series(1, 1200) AS gs;
                "#
            ))
            .await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        db.client.batch_execute("RESET koldstore.user_id").await?;
        let flushed_a = force_flush(&db, &relation).await?;
        anyhow::ensure!(flushed_a > 0, "user-a flush archived no rows");

        // Wave B: only user-b → separate segment(s) with user_id = user-b.
        db.client
            .batch_execute("SET koldstore.user_id = 'user-b'")
            .await?;
        db.client
            .batch_execute(&format!(
                r#"
                INSERT INTO {relation} (id, user_id, title, body)
                SELECT gs, 'user-b', 'b-' || gs, 'body-b'
                FROM generate_series(2001, 3200) AS gs;
                "#
            ))
            .await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        db.client.batch_execute("RESET koldstore.user_id").await?;
        let flushed_b = force_flush(&db, &relation).await?;
        anyhow::ensure!(flushed_b > 0, "user-b flush archived no rows");

        let segments: i64 = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.cold_segments
                WHERE table_oid = $1::text::regclass::oid
                  AND status = 'active'
                "#,
                &[&relation],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            segments >= 2,
            "expected >=2 cold segments after segregated flushes, got {segments}"
        );

        // Catalog must have scope-column stats (indexed path, not manifest.json).
        let stats_rows: i64 = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.cold_segment_stats
                WHERE table_oid = $1::text::regclass::oid
                  AND column_name = 'user_id'
                "#,
                &[&relation],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            stats_rows >= 2,
            "expected user_id cold_segment_stats rows, got {stats_rows}"
        );

        db.client
            .batch_execute("SET koldstore.user_id = 'user-a'")
            .await?;
        let plan = common::explain_analyze(
            &db.client,
            &format!(
                "SELECT id, title FROM {relation} WHERE user_id = 'user-a' ORDER BY id LIMIT 50"
            ),
        )
        .await?;
        common::assertions::assert_kold_merge_scan_explain(&plan)?;
        anyhow::ensure!(
            plan.contains("Segment Catalog Source: postgres (koldstore.cold_segments)"),
            "expected DB segment catalog source, got:\n{plan}"
        );
        anyhow::ensure!(
            plan.contains("Segments Pruned by Min/Max:"),
            "expected min/max prune counter, got:\n{plan}"
        );
        let pruned = explain_counter(&plan, "Segments Pruned by Min/Max")?;
        let opened = explain_counter(&plan, "Parquet Segments Opened")?;
        let considered = explain_counter(&plan, "Candidate Segments")?;
        anyhow::ensure!(
            pruned >= 1,
            "scope equality should prune at least one foreign-tenant segment; pruned={pruned} opened={opened} considered={considered}\n{plan}"
        );
        anyhow::ensure!(
            opened < considered,
            "opened ({opened}) should be < candidates ({considered}) after scope prune\n{plan}"
        );

        let visible: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*) FROM {relation} WHERE user_id = 'user-a'"),
                &[],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            visible == 1200,
            "scoped count must remain correct after prune, got {visible}"
        );
    }
    Ok(())
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
            "SELECT COALESCE(rows_flushed, 0)::bigint FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        )
        .await?;
    Ok(progress.get(0))
}

fn explain_counter(plan: &str, label: &str) -> Result<usize> {
    for line in plan.lines() {
        let trimmed = line.trim();
        let prefix = format!("{label}: ");
        if let Some(rest) = trimmed.strip_prefix(&prefix) {
            let value = rest
                .split_whitespace()
                .next()
                .ok_or_else(|| anyhow::anyhow!("empty counter for {label} in:\n{plan}"))?;
            return value
                .parse()
                .map_err(|error| anyhow::anyhow!("parse {label}={value}: {error}"));
        }
    }
    anyhow::bail!("missing `{label}` in plan:\n{plan}")
}

#[tokio::test]
async fn user_scope_cold_segment_lookup_is_index_backed_on_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "user_scope_cold_pruning").await?;
        let table = db.create_user_notes_table("scope_notes").await?;
        db.manage_user_scoped(&table.relation, "user_id").await?;

        let plan = common::explain_with_seqscan_disabled(
            &db.client,
            &format!(
                r#"
                SELECT object_path
                FROM koldstore.cold_segments
                WHERE table_oid = '{}'::regclass::oid
                  AND scope_key = 'user-a'
                  AND status = 'active'
                  AND min_seq <= 5
                  AND max_seq >= 5
                ORDER BY min_seq, max_seq
                "#,
                table.relation
            ),
        )
        .await?;
        common::assertions::assert_catalog_index_plan_uses_any(
            &plan,
            &[
                "cold_segments_active_scope_seq_idx",
                "cold_segments_active_commit_idx",
            ],
        )?;
        assert!(plan.contains("scope_key = 'user-a'::text"), "{plan}");
        assert!(plan.contains("min_seq <= 5"), "{plan}");
        assert!(plan.contains("max_seq >= 5"), "{plan}");

        let rows = db
            .client
            .query(
                r#"
                SELECT object_path
                FROM koldstore.cold_segments
                WHERE table_oid = $1::text::regclass::oid
                  AND scope_key = 'user-a'
                  AND status = 'active'
                "#,
                &[&table.relation],
            )
            .await?;
        assert_eq!(rows.len(), 0);
    }

    Ok(())
}
