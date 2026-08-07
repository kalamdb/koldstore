//! WAL-only seq cursor coverage for issue #71.
//!
//! Focused activation, commit-order seq assignment, `koldstore.changes_since`
//! pagination (hot + cold, mid-cursor spans, newest rewind), watermark
//! continuity across worker restart, and flush cursor continuity.

use crate::common;
use crate::flush::harness::connect_peer;

use anyhow::{bail, Context, Result};
use koldstore_common::QualifiedTableName;
use koldstore_merge::events;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

#[tokio::test]
async fn wal_only_empty_activation_has_no_capture_triggers() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_empty_activation").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_empty", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_table(&db.client, &relation, &db.storage_name).await?;

        let triggers = source_triggers(&db.client, &relation).await?;
        assert_eq!(
            triggers,
            vec![format!("{table_name}__cl_pk_update_guard")],
            "WAL-only activation must not install DML capture triggers"
        );

        let state: String = db
            .client
            .query_one(
                "SELECT initialization_state FROM koldstore.schemas \
                 WHERE table_oid = $1::text::regclass",
                &[&relation],
            )
            .await?
            .get(0);
        assert_eq!(state, "complete");

        let active: bool = db
            .client
            .query_one(
                "SELECT active FROM koldstore.schemas WHERE table_oid = $1::text::regclass",
                &[&relation],
            )
            .await?
            .get(0);
        assert!(active);

        db.client
            .execute(
                &format!("INSERT INTO {relation} VALUES (1, 'after-activation')"),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 1).await?;
        assert_eq!(common::wait_for_async_mirror(&db.client).await?, 0);

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn wal_apply_assigns_seq_in_commit_order_not_start_order() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_commit_order_seq").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_order", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_table(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        let early_starter = connect_peer(&db).await?;
        let late_starter = connect_peer(&db).await?;

        // Start txn A first, then txn B; commit B before A so commit order ≠ start order.
        early_starter.batch_execute("BEGIN").await?;
        early_starter
            .execute(
                &format!("INSERT INTO {relation} VALUES (1, 'started-first')"),
                &[],
            )
            .await?;

        late_starter.batch_execute("BEGIN").await?;
        late_starter
            .execute(
                &format!("INSERT INTO {relation} VALUES (2, 'started-second')"),
                &[],
            )
            .await?;
        late_starter.batch_execute("COMMIT").await?;
        early_starter.batch_execute("COMMIT").await?;

        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 2).await?;
        common::wait_for_async_mirror(&db.client).await?;

        let rows = db
            .client
            .query(&format!("SELECT id, seq FROM {mirror} ORDER BY id"), &[])
            .await?;
        assert_eq!(rows.len(), 2);
        let seq1: i64 = rows[0].get(1);
        let seq2: i64 = rows[1].get(1);
        assert!(
            seq2 < seq1,
            "id=2 committed first so its applied seq ({seq2}) must be less than id=1 seq ({seq1})"
        );

        let watermark: i64 = db
            .client
            .query_one(
                "SELECT seq_high_watermark FROM koldstore.async_mirror_state LIMIT 1",
                &[],
            )
            .await?
            .get(0);
        assert!(
            watermark >= seq1,
            "durable watermark {watermark} must cover applied seq {seq1}"
        );

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_seq_pagination_survives_flush() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_changes_since_flush").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_feed", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_table(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'v1-' || id FROM generate_series(1, 50) id"
                ),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 50).await?;
        common::wait_for_async_mirror(&db.client).await?;

        // Coalesce repeated updates into latest-state rows.
        db.client
            .execute(
                &format!("UPDATE {relation} SET body = 'v2-' || id WHERE id <= 10"),
                &[],
            )
            .await?;
        db.client
            .execute(
                &format!("UPDATE {relation} SET body = 'v3-' || id WHERE id <= 10"),
                &[],
            )
            .await?;
        common::wait_for_async_mirror(&db.client).await?;

        let page_sizes = [1_i64, 10, 25];
        let mut observed = Vec::new();
        let mut last_seq = 0_i64;
        loop {
            let page = db
                .client
                .query(
                    &format!(
                        "SELECT id, seq FROM {mirror} WHERE seq > $1 ORDER BY seq ASC LIMIT $2"
                    ),
                    &[&last_seq, &page_sizes[observed.len() % page_sizes.len()]],
                )
                .await?;
            if page.is_empty() {
                break;
            }
            for row in &page {
                let id: i64 = row.get(0);
                let seq: i64 = row.get(1);
                assert!(seq > last_seq, "pagination must be exclusive on seq");
                observed.push((id, seq));
                last_seq = seq;
            }
        }
        assert_eq!(observed.len(), 50, "latest-state feed has one row per PK");
        let mut ids: Vec<_> = observed.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 50, "no duplicate PKs across pages");

        let cursor_before_flush = last_seq;
        common::flush_table_job_id(&db.client, &relation, true).await?;

        // Exact versions selected for flush are pruned; cursor must remain valid
        // for any newer mirror versions.
        db.client
            .execute(
                &format!("INSERT INTO {relation} VALUES (51, 'post-flush')"),
                &[],
            )
            .await?;
        common::wait_for_async_mirror(&db.client).await?;
        let after = db
            .client
            .query(
                &format!("SELECT id, seq FROM {mirror} WHERE seq > $1 ORDER BY seq"),
                &[&cursor_before_flush],
            )
            .await?;
        assert!(
            after.iter().any(|row| row.get::<_, i64>(0) == 51),
            "changes_since cursor must remain valid across flush"
        );
        for row in &after {
            let seq: i64 = row.get(1);
            assert!(seq > cursor_before_flush);
        }

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn worker_restart_keeps_seq_above_durable_watermark() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_watermark_restart").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_wm", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_table(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!("INSERT INTO {relation} SELECT id, 'a' FROM generate_series(1, 20) id"),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 20).await?;
        common::wait_for_async_mirror(&db.client).await?;

        let watermark_before: i64 = db
            .client
            .query_one(
                "SELECT seq_high_watermark FROM koldstore.async_mirror_state LIMIT 1",
                &[],
            )
            .await?
            .get(0);
        assert!(watermark_before > 0);

        common::terminate_async_worker(&db.client).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!("INSERT INTO {relation} SELECT id, 'b' FROM generate_series(21, 30) id"),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 30).await?;
        common::wait_for_async_mirror(&db.client).await?;

        let min_new: i64 = db
            .client
            .query_one(
                &format!("SELECT min(seq) FROM {mirror} WHERE id >= 21"),
                &[],
            )
            .await?
            .get(0);
        assert!(
            min_new > watermark_before,
            "post-restart seq {min_new} must stay above durable watermark {watermark_before}"
        );

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn populated_activation_under_concurrent_dml_is_gap_free() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_populated_activation").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_pop", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'seed-' || id FROM generate_series(1, 200) id"
                ),
                &[],
            )
            .await?;

        let peer = connect_peer(&db).await?;
        let writer = tokio::spawn({
            let relation = relation.clone();
            async move {
                for id in 201_i64..=250 {
                    peer.execute(
                        &format!("INSERT INTO {relation} (id, body) VALUES ($1, $2)"),
                        &[&id, &format!("concurrent-{id}")],
                    )
                    .await?;
                    if id % 5 == 0 {
                        let target = id - 4;
                        peer.execute(
                            &format!("UPDATE {relation} SET body = $2 WHERE id = $1"),
                            &[&target, &format!("upd-{target}")],
                        )
                        .await?;
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
        });

        manage_table(&db.client, &relation, &db.storage_name).await?;
        writer.await??;

        let triggers = source_triggers(&db.client, &relation).await?;
        assert_eq!(
            triggers,
            vec![format!("{table_name}__cl_pk_update_guard")],
            "populated activation must remain WAL-only"
        );

        common::wait_for_async_worker(&db.client).await?;
        common::wait_for_async_mirror(&db.client).await?;

        let heap_count: i64 = db
            .client
            .query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[])
            .await?
            .get(0);
        let mirror_count: i64 = db
            .client
            .query_one(&format!("SELECT count(*)::bigint FROM {mirror}"), &[])
            .await?
            .get(0);
        assert_eq!(
            heap_count, mirror_count,
            "activation+catch-up must leave no capture gap"
        );
        assert_eq!(heap_count, 250);

        let state: String = db
            .client
            .query_one(
                "SELECT initialization_state FROM koldstore.schemas \
                 WHERE table_oid = $1::text::regclass",
                &[&relation],
            )
            .await?
            .get(0);
        assert_eq!(state, "complete");

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_hot_mirror_index_scan_for_seq_pagination() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_changes_since_idx").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_idx", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");
        let seq_index = format!("{table_name}__cl_seq_idx");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_table(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'row-' || id FROM generate_series(1, 5000) id"
                ),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 5000).await?;
        common::wait_for_async_mirror(&db.client).await?;

        let indexes: Vec<String> = db
            .client
            .query(
                "SELECT indexname::text FROM pg_indexes \
                 WHERE schemaname = 'koldstore' AND tablename = $1 \
                 ORDER BY indexname",
                &[&format!("{table_name}__cl")],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert!(
            indexes.iter().any(|name| name == &seq_index),
            "expected {seq_index} among {indexes:?}"
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == &format!("{table_name}__cl_tombstone_seq_idx")),
            "expected tombstone seq index among {indexes:?}"
        );

        let plan = common::explain_with_seqscan_disabled(
            &db.client,
            &format!("SELECT id, seq, op FROM {mirror} WHERE seq > 0 ORDER BY seq ASC LIMIT 100"),
        )
        .await?;
        common::assert_index_scan(&plan, &seq_index)?;

        let mirror_qtn = QualifiedTableName::parse(&mirror)?;
        let planned = events::plan_mirror_changes_since(&mirror_qtn, &["id".to_string()], None)?;
        assert!(
            planned
                .statement
                .sql
                .contains("mirror.\"seq\" > $1::bigint"),
            "planned feed SQL must use exclusive seq cursor"
        );
        assert!(
            planned
                .statement
                .sql
                .contains("ORDER BY mirror.\"seq\" ASC"),
            "planned feed SQL must order by seq"
        );
        assert!(
            !planned.statement.sql.contains("row_events"),
            "planned feed SQL must not touch legacy row_events"
        );
        let planned_explain_sql = planned
            .statement
            .sql
            .replace("$1::bigint", "0")
            .replace("$3::integer", "100");
        let planned_plan =
            common::explain_with_seqscan_disabled(&db.client, &planned_explain_sql).await?;
        common::assert_index_scan(&planned_plan, &seq_index)?;

        let page = page_changes_since(&db.client, &mirror, 0, 100).await?;
        assert_eq!(page.len(), 100);
        assert!(page.windows(2).all(|w| w[0].seq < w[1].seq));

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_pagination_no_skip_or_dup_under_concurrent_dml() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_changes_since_race").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_race", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_table(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'seed-' || id FROM generate_series(1, 100) id"
                ),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 100).await?;
        common::wait_for_async_mirror(&db.client).await?;

        let peer = connect_peer(&db).await?;
        let writer = tokio::spawn({
            let relation = relation.clone();
            async move {
                for id in 101_i64..=300 {
                    peer.execute(
                        &format!("INSERT INTO {relation} (id, body) VALUES ($1, $2)"),
                        &[&id, &format!("live-{id}")],
                    )
                    .await?;
                    if id % 7 == 0 {
                        let target = id - 50;
                        if target >= 1 {
                            peer.execute(
                                &format!("UPDATE {relation} SET body = $2 WHERE id = $1"),
                                &[&target, &format!("bump-{target}")],
                            )
                            .await?;
                        }
                    }
                    if id % 11 == 0 {
                        let target = id - 30;
                        if (1..=100).contains(&target) {
                            peer.execute(
                                &format!("DELETE FROM {relation} WHERE id = $1"),
                                &[&target],
                            )
                            .await?;
                            peer.execute(
                                &format!("INSERT INTO {relation} (id, body) VALUES ($1, $2)"),
                                &[&target, &format!("revive-{target}")],
                            )
                            .await?;
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
        });

        let mut observed: BTreeMap<i64, (i64, i16)> = BTreeMap::new();
        let mut last_seq = 0_i64;
        loop {
            let page = page_changes_since(&db.client, &mirror, last_seq, 17).await?;
            if page.is_empty() {
                if writer.is_finished() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _ = common::wait_for_async_mirror(&db.client).await?;
                continue;
            }
            for row in page {
                assert!(
                    row.seq > last_seq,
                    "exclusive seq cursor must advance: got {} after {}",
                    row.seq,
                    last_seq
                );
                last_seq = row.seq;
                observed.insert(row.id, (row.seq, row.op));
            }
        }
        writer.await??;
        common::wait_for_async_mirror(&db.client).await?;

        // Drain anything applied after the reader observed an empty page.
        loop {
            let page = page_changes_since(&db.client, &mirror, last_seq, 17).await?;
            if page.is_empty() {
                break;
            }
            for row in page {
                assert!(row.seq > last_seq);
                last_seq = row.seq;
                observed.insert(row.id, (row.seq, row.op));
            }
        }

        let final_rows = db
            .client
            .query(
                &format!("SELECT id, seq, op FROM {mirror} ORDER BY seq"),
                &[],
            )
            .await?;
        assert_eq!(
            observed.len(),
            final_rows.len(),
            "paged feed must cover every live latest-state mirror row"
        );
        let mut expected_seqs = BTreeSet::new();
        for row in &final_rows {
            let id: i64 = row.get(0);
            let seq: i64 = row.get(1);
            let op: i16 = row.get(2);
            expected_seqs.insert(seq);
            let Some((got_seq, got_op)) = observed.get(&id) else {
                bail!("missing id={id} from paged changes_since feed");
            };
            assert_eq!(*got_seq, seq);
            assert_eq!(*got_op, op);
        }
        let observed_seqs: BTreeSet<_> = observed.values().map(|(seq, _)| *seq).collect();
        assert_eq!(observed_seqs, expected_seqs);

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_includes_delete_revive_and_omits_rollback() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_changes_since_lifecycle").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_life", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_table(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!("INSERT INTO {relation} VALUES (1, 'v1'), (2, 'keep')"),
                &[],
            )
            .await?;
        common::wait_for_async_mirror(&db.client).await?;

        db.client
            .execute(
                &format!("UPDATE {relation} SET body = 'v2' WHERE id = 1"),
                &[],
            )
            .await?;
        db.client
            .execute(&format!("DELETE FROM {relation} WHERE id = 1"), &[])
            .await?;
        db.client
            .execute(
                &format!("INSERT INTO {relation} VALUES (1, 'revived')"),
                &[],
            )
            .await?;
        common::wait_for_async_mirror(&db.client).await?;

        let before_rollback = page_all_changes_since(&db.client, &mirror, 0, 10).await?;
        assert_eq!(
            before_rollback.len(),
            2,
            "latest-state feed is one row per PK"
        );
        let by_id: BTreeMap<_, _> = before_rollback
            .iter()
            .map(|row| (row.id, row.clone()))
            .collect();
        assert_eq!(
            by_id[&1].op, 1,
            "revive must surface as insert latest-state"
        );
        assert_eq!(by_id[&2].op, 1);
        assert_ne!(by_id[&1].seq, by_id[&2].seq);

        let cursor = before_rollback.iter().map(|r| r.seq).max().unwrap_or(0);
        db.client
            .batch_execute(&format!(
                r#"
                BEGIN;
                UPDATE {relation} SET body = 'rolled-back' WHERE id = 2;
                DELETE FROM {relation} WHERE id = 1;
                ROLLBACK;
                "#
            ))
            .await?;
        common::wait_for_async_mirror(&db.client).await?;

        let after_rollback = page_changes_since(&db.client, &mirror, cursor, 100).await?;
        assert!(
            after_rollback.is_empty(),
            "rolled-back mutations must not appear on the change feed, got {after_rollback:?}"
        );
        let still = page_all_changes_since(&db.client, &mirror, 0, 10).await?;
        assert_eq!(still.len(), 2);
        assert_eq!(still.iter().find(|r| r.id == 1).unwrap().op, 1);
        assert_eq!(still.iter().find(|r| r.id == 2).unwrap().op, 1);

        // Delete that commits must appear as latest-state tombstone.
        db.client
            .execute(&format!("DELETE FROM {relation} WHERE id = 2"), &[])
            .await?;
        common::wait_for_async_mirror(&db.client).await?;
        let with_delete = page_all_changes_since(&db.client, &mirror, 0, 10).await?;
        assert_eq!(with_delete.len(), 2);
        assert_eq!(with_delete.iter().find(|r| r.id == 2).unwrap().op, 3);

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_varied_limits_cover_all_live_latest_state() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_changes_since_limits").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_lim", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_table(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        const ROWS: i64 = 2500;
        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'v1-' || id FROM generate_series(1, {ROWS}) id"
                ),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, ROWS).await?;
        db.client
            .execute(
                &format!("UPDATE {relation} SET body = 'v2-' || id WHERE id % 3 = 0"),
                &[],
            )
            .await?;
        common::wait_for_async_mirror(&db.client).await?;

        for &limit in &[1_i64, 17, 100, 1000] {
            let pages = page_all_changes_since(&db.client, &mirror, 0, limit).await?;
            assert_eq!(
                pages.len() as i64,
                ROWS,
                "limit={limit} must still return every latest-state row"
            );
            let mut prev_seq = 0_i64;
            let mut ids = BTreeSet::new();
            for row in &pages {
                assert!(row.seq > prev_seq);
                prev_seq = row.seq;
                assert!(ids.insert(row.id), "duplicate id={} across pages", row.id);
            }
            assert_eq!(ids.len() as i64, ROWS);
        }

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_flush_prune_exposes_retention_floor_not_silent_catchup() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_changes_since_gap").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_gap", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  min_flush_rows => 1,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'pre-' || id FROM generate_series(1, 40) id"
                ),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 40).await?;
        common::wait_for_async_mirror(&db.client).await?;

        let pre_flush = db
            .client
            .query(
                "SELECT seq, (pk->>'id')::bigint AS id, op, source \
                 FROM koldstore.changes_since($1::text::regclass, 0, 100) \
                 ORDER BY seq",
                &[&relation],
            )
            .await?;
        assert_eq!(pre_flush.len(), 40);
        let flushed_max_seq: i64 = pre_flush
            .iter()
            .map(|row| row.get::<_, i64>(0))
            .max()
            .unwrap();
        assert!(pre_flush.iter().all(|row| row.get::<_, String>(3) == "hot"));

        common::flush_table_job_id(&db.client, &relation, true).await?;

        let hot_after: i64 = db
            .client
            .query_one(&format!("SELECT count(*)::bigint FROM {mirror}"), &[])
            .await?
            .get(0);
        assert_eq!(hot_after, 0, "force flush must prune flushed mirror rows");

        // Packaged API must still return flushed latest-state from cold.
        let after_flush = db
            .client
            .query(
                "SELECT seq, (pk->>'id')::bigint AS id, op, source \
                 FROM koldstore.changes_since($1::text::regclass, 0, 100) \
                 ORDER BY seq",
                &[&relation],
            )
            .await?;
        assert_eq!(after_flush.len(), 40);
        assert!(after_flush
            .iter()
            .all(|row| row.get::<_, String>(3) == "cold"));
        let ids: BTreeSet<i64> = after_flush.iter().map(|row| row.get(1)).collect();
        assert_eq!(ids.len(), 40);

        let cold_min: i64 = db
            .client
            .query_one(
                "SELECT min(min_seq)::bigint FROM koldstore.cold_segments \
                 WHERE table_oid = $1::text::regclass AND status = 'active'",
                &[&relation],
            )
            .await?
            .get(0);
        assert!(cold_min > 0);

        // A real (non-zero) cursor below the retained floor must error.
        if cold_min > 2 {
            let stale = db
                .client
                .query(
                    "SELECT * FROM koldstore.changes_since($1::text::regclass, $2::bigint, 10)",
                    &[&relation, &(cold_min - 2)],
                )
                .await;
            assert!(
                stale.is_err(),
                "stale positive cursor must raise a retention gap"
            );
        }

        db.client
            .execute(
                &format!("INSERT INTO {relation} VALUES (41, 'post-flush')"),
                &[],
            )
            .await?;
        common::wait_for_async_mirror(&db.client).await?;
        let after = db
            .client
            .query(
                "SELECT (pk->>'id')::bigint, seq, source \
                 FROM koldstore.changes_since($1::text::regclass, $2::bigint, 10) \
                 ORDER BY seq",
                &[&relation, &flushed_max_seq],
            )
            .await?;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].get::<_, i64>(0), 41);
        assert!(after[0].get::<_, i64>(1) > flushed_max_seq);
        assert_eq!(after[0].get::<_, String>(2), "hot");

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_merges_cold_oldest_and_hot_newest_with_mid_cursor() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_cs_hot_cold").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_mix", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 25,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;

        // Seed a cold generation, then leave a hot tail + newer inserts.
        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} \
                     SELECT id, 'pre-' || id FROM generate_series(1, 80) id"
                ),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 80).await?;
        common::wait_for_async_mirror(&db.client).await?;
        common::flush_table_job_id(&db.client, &relation, true)
            .await?
            .context("force flush seed batch")?;

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} \
                     SELECT id, 'post-' || id FROM generate_series(81, 100) id"
                ),
                &[],
            )
            .await?;
        common::fence_async_mirror(&db.client).await?;
        let mirror_rows: i64 = db
            .client
            .query_one(&format!("SELECT count(*)::bigint FROM {mirror}"), &[])
            .await?
            .get(0);
        assert!(
            mirror_rows >= 20,
            "expected hot mirror tail after post-flush inserts, got {mirror_rows}"
        );

        let hot_count = common::hot_row_count(&db.client, &relation).await?;
        assert!(
            hot_count > 0,
            "expected a hot tail after flush + inserts, got hot={hot_count}"
        );
        let cold_count: i64 = db
            .client
            .query_one(
                "SELECT coalesce(sum(row_count), 0)::bigint \
                 FROM koldstore.cold_segments \
                 WHERE table_oid = $1::text::regclass AND status = 'active'",
                &[&relation],
            )
            .await?
            .get(0);
        assert!(
            cold_count > 0,
            "expected cold segments after flush, got cold_rows={cold_count}"
        );

        // From start: oldest rows are cold, newest are hot.
        let full = db
            .client
            .query(
                "SELECT seq, (pk->>'id')::bigint AS id, source \
                 FROM koldstore.changes_since($1::text::regclass, 0, 1000) \
                 ORDER BY seq",
                &[&relation],
            )
            .await?;
        assert_eq!(
            full.len(),
            100,
            "feed must cover cold + hot rows for all PKs"
        );
        let sources: BTreeSet<String> = full.iter().map(|row| row.get(2)).collect();
        assert!(
            sources.contains("cold") && sources.contains("hot"),
            "full feed must mix cold oldest and hot newest, got {sources:?}"
        );
        let oldest_source: String = full[0].get(2);
        let newest_source: String = full[full.len() - 1].get(2);
        assert_eq!(oldest_source, "cold", "oldest retained change must be cold");
        assert_eq!(newest_source, "hot", "newest change must be hot");

        let first_hot_seq = full
            .iter()
            .find(|row| row.get::<_, String>(2) == "hot")
            .map(|row| row.get::<_, i64>(0))
            .context("expected at least one hot change")?;
        let last_cold_seq = full
            .iter()
            .rev()
            .find(|row| row.get::<_, String>(2) == "cold")
            .map(|row| row.get::<_, i64>(0))
            .context("expected at least one cold change")?;
        assert!(
            last_cold_seq < first_hot_seq,
            "cold seqs must precede hot seqs (last_cold={last_cold_seq}, first_hot={first_hot_seq})"
        );

        // Mid-cursor inside cold history: page must continue through cold into hot.
        let mid_cursor = last_cold_seq.saturating_sub(5).max(1);
        let spanning = db
            .client
            .query(
                "SELECT seq, (pk->>'id')::bigint AS id, source \
                 FROM koldstore.changes_since($1::text::regclass, $2::bigint, 40) \
                 ORDER BY seq",
                &[&relation, &mid_cursor],
            )
            .await?;
        assert!(
            !spanning.is_empty(),
            "mid-cursor page must return rows after seq={mid_cursor}"
        );
        assert!(spanning.iter().all(|row| row.get::<_, i64>(0) > mid_cursor));
        let span_sources: BTreeSet<String> = spanning.iter().map(|row| row.get(2)).collect();
        assert!(
            span_sources.contains("cold") && span_sources.contains("hot"),
            "mid-cursor page must include both cold and hot sources, got {span_sources:?} \
             (cursor={mid_cursor}, last_cold={last_cold_seq}, first_hot={first_hot_seq})"
        );

        // Newest-N rewind must land on the hot post-flush inserts.
        let newest = db
            .client
            .query(
                "SELECT (pk->>'id')::bigint AS id, source \
                 FROM koldstore.changes_since($1::text::regclass, 0, 1000, 5) \
                 ORDER BY seq",
                &[&relation],
            )
            .await?;
        assert_eq!(newest.len(), 5);
        let newest_ids: Vec<i64> = newest.iter().map(|row| row.get(0)).collect();
        assert_eq!(newest_ids, vec![96, 97, 98, 99, 100]);
        assert!(
            newest.iter().all(|row| row.get::<_, String>(1) == "hot"),
            "last_rows rewind must come from the hot tail"
        );

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_limit_does_not_open_newer_unneeded_segments() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_changes_bounded").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_bounded", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;

        for wave in 0..3_i64 {
            let start = wave * 1000 + 1;
            let end = start + 999;
            db.client
                .batch_execute(&format!(
                    "INSERT INTO {relation} \
                     SELECT id, 'row-' || id FROM generate_series({start}, {end}) id"
                ))
                .await?;
            // Prior waves may already be pruned from the mirror; wait for this
            // wave's inserts only.
            common::fence_async_mirror(&db.client).await?;
            let wave_rows: i64 = db
                .client
                .query_one(
                    &format!(
                        "SELECT count(*)::bigint FROM {mirror} \
                         WHERE op = 1 AND id BETWEEN {start} AND {end}"
                    ),
                    &[],
                )
                .await?
                .get(0);
            assert_eq!(
                wave_rows, 1000,
                "wave {wave} must be fully mirrored before flush, got {wave_rows}"
            );
            let flushed = common::flush_table_job_id(&db.client, &relation, true)
                .await?
                .context("force flush must return a job id")?;
            assert!(!flushed.is_empty());
        }

        let cold_segments = db
            .client
            .query(
                &format!(
                    r#"
                    SELECT {object}, cs.min_seq, cs.max_seq
                    FROM koldstore.cold_segments cs
                    JOIN pg_class c ON c.oid = cs.table_oid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE cs.table_oid = $1::text::regclass::oid
                      AND cs.status = 'active'
                    ORDER BY cs.min_seq, cs.max_seq, cs.path
                    "#,
                    object = common::SQL_DEFAULT_COLD_OBJECT_KEY,
                ),
                &[&relation],
            )
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    row.get::<_, i64>(1),
                    row.get::<_, i64>(2),
                )
            })
            .collect::<Vec<_>>();
        assert!(
            cold_segments.len() >= 2,
            "expected at least two cold segments after three flush waves, got {}",
            cold_segments.len()
        );

        // Park the newest segment by max_seq so a correct first page must not open it.
        let newest_idx = cold_segments
            .iter()
            .enumerate()
            .max_by_key(|(_, segment)| (segment.2, segment.1))
            .map(|(idx, _)| idx)
            .context("cold segments must be non-empty")?;
        let newest = &cold_segments[newest_idx];
        let oldest_max = cold_segments
            .iter()
            .map(|segment| segment.2)
            .min()
            .context("cold segments must be non-empty")?;
        let unneeded = db.storage_root.join(&newest.0);
        let parked = unneeded.with_extension("parquet.parked");
        std::fs::rename(&unneeded, &parked)?;
        let first_page = db
            .client
            .query(
                "SELECT (pk->>'id')::bigint AS id \
                 FROM koldstore.changes_since($1::text::regclass, 0, 25) \
                 ORDER BY seq",
                &[&relation],
            )
            .await;
        let cursor = oldest_max - 10;
        let spanning_page = db
            .client
            .query(
                "SELECT seq \
                 FROM koldstore.changes_since($1::text::regclass, $2, 25) \
                 ORDER BY seq",
                &[&relation, &cursor],
            )
            .await;
        std::fs::rename(&parked, &unneeded)?;

        let ids = first_page?
            .into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        assert_eq!(ids, (1..=25).collect::<Vec<_>>());

        let spanning_seqs = spanning_page?
            .into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        assert_eq!(spanning_seqs.len(), 25);
        assert!(spanning_seqs.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(spanning_seqs.iter().all(|seq| *seq > cursor));
        assert!(
            spanning_seqs.iter().any(|seq| *seq > oldest_max),
            "page should continue past the oldest segment max_seq={oldest_max}"
        );

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_from_start_keeps_cold_before_hot_across_segments() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_cs_cold_then_hot").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_order", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 20,
                  min_flush_rows => 1,
                  max_rows_per_file => 1000,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;

        // Small first cold generation — historically enough for hot to pad a
        // limit=25 page and skip later cold groups.
        db.client
            .batch_execute(&format!(
                "INSERT INTO {relation} \
                 SELECT id, 'cold-a-' || id FROM generate_series(1, 12) id"
            ))
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 12).await?;
        common::wait_for_async_mirror(&db.client).await?;
        common::flush_table_job_id(&db.client, &relation, true)
            .await?
            .context("flush first cold wave")?;

        db.client
            .batch_execute(&format!(
                "INSERT INTO {relation} \
                 SELECT id, 'cold-b-' || id FROM generate_series(13, 80) id"
            ))
            .await?;
        // First-wave mirror rows may already be pruned; wait until the new batch
        // is visible (exact residual count is flush-dependent).
        common::fence_async_mirror(&db.client).await?;
        let second_wave: i64 = db
            .client
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM {mirror} \
                     WHERE op = 1 AND id BETWEEN 13 AND 80"
                ),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(
            second_wave, 68,
            "second cold wave must be mirrored before flush, got {second_wave}"
        );
        common::flush_table_job_id(&db.client, &relation, true)
            .await?
            .context("flush second cold wave")?;

        db.client
            .batch_execute(&format!(
                "INSERT INTO {relation} \
                 SELECT id, 'hot-' || id FROM generate_series(81, 120) id"
            ))
            .await?;
        common::fence_async_mirror(&db.client).await?;

        let hot_count = common::hot_row_count(&db.client, &relation).await?;
        assert!(
            hot_count > 0,
            "expected a hot tail after post-flush inserts, got hot={hot_count}"
        );
        let cold_segments: i64 = db
            .client
            .query_one(
                "SELECT count(*)::bigint FROM koldstore.cold_segments \
                 WHERE table_oid = $1::text::regclass AND status = 'active'",
                &[&relation],
            )
            .await?
            .get(0);
        assert!(
            cold_segments >= 2,
            "need multiple cold generations to exercise group early-exit, got {cold_segments}"
        );

        let page = db
            .client
            .query(
                "SELECT seq, (pk->>'id')::bigint AS id, source \
                 FROM koldstore.changes_since($1::text::regclass, 0, 25) \
                 ORDER BY seq",
                &[&relation],
            )
            .await?;
        assert_eq!(page.len(), 25, "first page must be full");
        let ids = page
            .iter()
            .map(|row| row.get::<_, i64>(1))
            .collect::<Vec<_>>();
        let sources = page
            .iter()
            .map(|row| row.get::<_, String>(2))
            .collect::<Vec<_>>();
        let seqs = page
            .iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();

        assert_eq!(
            ids,
            (1..=25).collect::<Vec<_>>(),
            "since_seq=0 must deliver oldest retained rows first; got ids={ids:?} sources={sources:?}"
        );
        assert!(
            sources.iter().all(|source| source == "cold"),
            "first page must stay entirely in cold while older cold remains, got {sources:?}"
        );
        assert!(
            seqs.windows(2).all(|pair| pair[0] < pair[1]),
            "seq must be strictly ascending: {seqs:?}"
        );

        let full = drain_changes_since_feed(&db.client, &relation, 0, 40).await?;
        assert!(
            full.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "full drain must stay seq-ordered"
        );
        let first_hot = full
            .iter()
            .position(|row| row.2 == "hot")
            .context("expected hot rows after cold")?;
        assert!(
            full[..first_hot].iter().all(|row| row.2 == "cold"),
            "hot must not appear before older cold rows"
        );
        assert!(
            full[first_hot..].iter().all(|row| row.2 == "hot"),
            "once hot starts, remaining rows should be the hot tail"
        );
        // Pure seq cursor may emit more than one event per PK (e.g. cold insert
        // then a later hot version). Coverage is unique live PKs, not event count.
        let unique_ids: BTreeSet<i64> = full.iter().map(|row| row.1).collect();
        assert_eq!(
            unique_ids,
            (1..=120).collect::<BTreeSet<_>>(),
            "full drain must cover every live PK (events={}, unique={})",
            full.len(),
            unique_ids.len()
        );
        assert!(
            full.len() >= 120,
            "seq feed should return at least one event per PK, got {}",
            full.len()
        );

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn changes_since_last_rows_rewinds_newest_n_like_kalamdb() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "wal_changes_last_rows").await?;
        ensure_publication(&db.client).await?;
        let table_name = format!("{}_last", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_table(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'row-' || id FROM generate_series(1, 30) id"
                ),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 30).await?;
        common::wait_for_async_mirror(&db.client).await?;

        let last = db
            .client
            .query(
                "SELECT (pk->>'id')::bigint AS id, seq \
                 FROM koldstore.changes_since($1::text::regclass, 0, 1000, 5) \
                 ORDER BY seq",
                &[&relation],
            )
            .await?;
        assert_eq!(last.len(), 5);
        let ids: Vec<i64> = last.iter().map(|row| row.get(0)).collect();
        assert_eq!(ids, vec![26, 27, 28, 29, 30]);
        assert!(
            last.windows(2)
                .all(|w| w[0].get::<_, i64>(1) < w[1].get::<_, i64>(1)),
            "last_rows must be delivered oldest→newest"
        );

        // Positive since_seq wins over last_rows (KalamDB precedence).
        let max_seq: i64 = last.last().unwrap().get(1);
        let resume = db
            .client
            .query(
                "SELECT (pk->>'id')::bigint \
                 FROM koldstore.changes_since($1::text::regclass, $2::bigint, 1000, 5)",
                &[&relation, &max_seq],
            )
            .await?;
        assert!(
            resume.is_empty(),
            "since_seq > 0 must ignore last_rows and resume exclusively"
        );

        // Flush then last_rows still sees cold winners.
        common::flush_table_job_id(&db.client, &relation, true).await?;
        let after_flush = db
            .client
            .query(
                "SELECT (pk->>'id')::bigint, source \
                 FROM koldstore.changes_since($1::text::regclass, 0, 1000, 3) \
                 ORDER BY seq",
                &[&relation],
            )
            .await?;
        assert_eq!(after_flush.len(), 3);
        assert_eq!(
            after_flush
                .iter()
                .map(|row| row.get::<_, i64>(0))
                .collect::<Vec<_>>(),
            vec![28, 29, 30]
        );
        assert!(after_flush
            .iter()
            .all(|row| row.get::<_, String>(1) == "cold"));

        unmanage(&db.client, &relation).await?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChangeRow {
    id: i64,
    seq: i64,
    op: i16,
}

async fn page_changes_since(
    client: &tokio_postgres::Client,
    mirror: &str,
    since_seq: i64,
    limit: i64,
) -> Result<Vec<ChangeRow>> {
    let rows = client
        .query(
            &format!(
                "SELECT id, seq, op FROM {mirror} \
                 WHERE seq > $1 ORDER BY seq ASC LIMIT $2"
            ),
            &[&since_seq, &limit],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| ChangeRow {
            id: row.get(0),
            seq: row.get(1),
            op: row.get(2),
        })
        .collect())
}

/// Pages `koldstore.changes_since` until empty (seq, id, source).
async fn drain_changes_since_feed(
    client: &tokio_postgres::Client,
    relation: &str,
    since_seq: i64,
    limit: i32,
) -> Result<Vec<(i64, i64, String)>> {
    let mut cursor = since_seq;
    let mut out = Vec::new();
    loop {
        let rows = client
            .query(
                "SELECT seq, (pk->>'id')::bigint AS id, source \
                 FROM koldstore.changes_since($1::text::regclass, $2, $3) \
                 ORDER BY seq",
                &[&relation, &cursor, &limit],
            )
            .await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let seq: i64 = row.get(0);
            cursor = seq;
            out.push((seq, row.get(1), row.get(2)));
        }
    }
    Ok(out)
}

async fn page_all_changes_since(
    client: &tokio_postgres::Client,
    mirror: &str,
    since_seq: i64,
    limit: i64,
) -> Result<Vec<ChangeRow>> {
    let mut out = Vec::new();
    let mut cursor = since_seq;
    loop {
        let page = page_changes_since(client, mirror, cursor, limit).await?;
        if page.is_empty() {
            break;
        }
        for row in page {
            cursor = row.seq;
            out.push(row);
        }
    }
    Ok(out)
}

async fn ensure_publication(client: &tokio_postgres::Client) -> Result<()> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'koldstore_async_mirror')",
            &[],
        )
        .await?
        .get(0);
    if !exists {
        client
            .batch_execute("CREATE PUBLICATION koldstore_async_mirror")
            .await?;
    }
    Ok(())
}

async fn manage_table(
    client: &tokio_postgres::Client,
    relation: &str,
    storage: &str,
) -> Result<()> {
    client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 1000,
              migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[&relation, &storage],
        )
        .await?;
    Ok(())
}

async fn unmanage(client: &tokio_postgres::Client, relation: &str) -> Result<()> {
    let _ = client
        .query_one(
            "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
            &[&relation],
        )
        .await;
    Ok(())
}

async fn source_triggers(client: &tokio_postgres::Client, relation: &str) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT tgname::text FROM pg_trigger \
             WHERE tgrelid = $1::text::regclass AND NOT tgisinternal \
             ORDER BY tgname",
            &[&relation],
        )
        .await?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}
