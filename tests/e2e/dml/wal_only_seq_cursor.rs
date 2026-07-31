//! WAL-only seq cursor coverage for issue #71.
//!
//! Focused activation, commit-order seq assignment, changes_since pagination,
//! watermark continuity across worker restart, and flush cursor continuity.

use crate::common;
use crate::flush::harness::connect_peer;

use anyhow::Result;
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
        db.client
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass, true)",
                &[&relation],
            )
            .await?;

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
