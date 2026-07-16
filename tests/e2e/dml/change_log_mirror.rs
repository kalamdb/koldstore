#[path = "../common/mod.rs"]
mod common;

use anyhow::Result;
use std::time::{Duration, Instant};

const MIRROR_DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn mirror_tracks_insert_update_delete_reinsert_and_rollback() -> Result<()> {
    let mode = common::selected_mirror_capture_mode()?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(
            target.clone(),
            &format!("change_log_mirror_{}", mode.as_str()),
        )
        .await?;
        let table_name = format!("{}_messages", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  body text NOT NULL
                );
                "#
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => 1000,
                  min_flush_rows => 1,
                  migration_order_by => 'id',
                  mirror_capture_mode => $3
                )
                "#,
                &[&relation, &db.storage_name, &mode.as_str()],
            )
            .await?;

        common::assert_system_columns_absent(&db.client, &relation).await?;
        common::assert_change_log_mirror_exists(&db.client, &mirror).await?;
        common::assert_primary_key_columns_match(&db.client, &relation, &mirror).await?;

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'one')"),
                &[],
            )
            .await?;
        let insert = wait_for_mirror_state(&db.client, &mirror, 1, 1).await?;
        assert_eq!(insert.op, 1);

        db.client
            .execute(
                &format!("UPDATE {relation} SET body = 'two' WHERE id = 1"),
                &[],
            )
            .await?;
        let update = wait_for_mirror_state(&db.client, &mirror, 1, 2).await?;
        assert_eq!(update.op, 2);
        assert!(update.seq > insert.seq);

        db.client
            .execute(&format!("DELETE FROM {relation} WHERE id = 1"), &[])
            .await?;
        let delete = wait_for_mirror_state(&db.client, &mirror, 1, 3).await?;
        assert_eq!(delete.op, 3);
        assert!(delete.seq > update.seq);

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'again')"),
                &[],
            )
            .await?;
        let reinsert = wait_for_mirror_state(&db.client, &mirror, 1, 1).await?;
        assert_eq!(reinsert.op, 1);
        assert!(reinsert.seq > delete.seq);
        assert_eq!(mirror_row_count(&db.client, &mirror, 1).await?, 1);
        let physical_mirror_rows = mirror_row_count(&db.client, &mirror, 1).await?;
        let reported = reported_mirror_counter(&db.client, &relation).await?;
        assert_eq!(physical_mirror_rows, reported);
        assert_eq!(reported, 1);

        db.client
            .batch_execute(&format!(
                r#"
                BEGIN;
                UPDATE {relation} SET body = 'rolled-back' WHERE id = 1;
                ROLLBACK;
                "#
            ))
            .await?;
        let after_rollback = mirror_state(&db.client, &mirror, 1).await?;
        assert_eq!(after_rollback, reinsert);

        db.client
            .execute(&format!("UPDATE {relation} SET id = id WHERE id = 1"), &[])
            .await?;
        let after_noop_pk = wait_for_mirror_state(&db.client, &mirror, 1, 2).await?;
        assert!(after_noop_pk.seq > reinsert.seq);

        let pk_update = db
            .client
            .execute(&format!("UPDATE {relation} SET id = 2 WHERE id = 1"), &[])
            .await;
        assert!(
            pk_update.is_err(),
            "managed primary-key updates must be rejected to avoid stale mirror rows"
        );
        assert_eq!(mirror_row_count(&db.client, &mirror, 1).await?, 1);
        assert_eq!(mirror_row_count(&db.client, &mirror, 2).await?, 0);
    }

    Ok(())
}

#[tokio::test]
async fn mirror_bulk_update_and_delete_keep_latest_state() -> Result<()> {
    let mode = common::selected_mirror_capture_mode()?;
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(
            target.clone(),
            &format!("change_log_mirror_bulk_{}", mode.as_str()),
        )
        .await?;
        let table_name = format!("{}_messages", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        db.client
            .batch_execute(&format!(
                r#"
                CREATE TABLE {relation} (
                  id bigint PRIMARY KEY,
                  body text NOT NULL
                );
                "#
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name     => $1::text::regclass,
                  storage        => $2,
                  hot_row_limit  => 10000,
                  min_flush_rows => 1,
                  migration_order_by => 'id',
                  mirror_capture_mode => $3
                )
                "#,
                &[&relation, &db.storage_name, &mode.as_str()],
            )
            .await?;
        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} (id, body)
                     SELECT g, 'row-' || g::text
                     FROM generate_series(1, 1000) AS g"
                ),
                &[],
            )
            .await?;
        wait_for_op_count(&db.client, &mirror, 1, 1_000).await?;
        let insert_max: i64 = db
            .client
            .query_one(&format!("SELECT max(seq) FROM {mirror}"), &[])
            .await?
            .get(0);

        db.client
            .execute(
                &format!("UPDATE {relation} SET body = 'updated-' || id::text"),
                &[],
            )
            .await?;
        wait_for_op_count(&db.client, &mirror, 2, 1_000).await?;
        let update_count: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {mirror} WHERE op = 2"), &[])
            .await?
            .get(0);
        assert_eq!(update_count, 1000);
        let update_min: i64 = db
            .client
            .query_one(&format!("SELECT min(seq) FROM {mirror}"), &[])
            .await?
            .get(0);
        assert!(update_min > insert_max);

        db.client
            .execute(&format!("DELETE FROM {relation}"), &[])
            .await?;
        wait_for_op_count(&db.client, &mirror, 3, 1_000).await?;
        let source_count: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
            .await?
            .get(0);
        let delete_count: i64 = db
            .client
            .query_one(&format!("SELECT count(*) FROM {mirror} WHERE op = 3"), &[])
            .await?
            .get(0);
        assert_eq!(source_count, 0);
        assert_eq!(delete_count, 1000);
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MirrorState {
    seq: i64,
    op: i16,
}

async fn mirror_state(
    client: &tokio_postgres::Client,
    mirror: &str,
    id: i64,
) -> Result<MirrorState> {
    let row = client
        .query_one(
            &format!("SELECT seq, op FROM {mirror} WHERE id = $1"),
            &[&id],
        )
        .await?;
    Ok(MirrorState {
        seq: row.get(0),
        op: row.get(1),
    })
}

async fn wait_for_mirror_state(
    client: &tokio_postgres::Client,
    mirror: &str,
    id: i64,
    expected_op: i16,
) -> Result<MirrorState> {
    let started = Instant::now();
    loop {
        if let Ok(state) = mirror_state(client, mirror, id).await {
            if state.op == expected_op {
                return Ok(state);
            }
        }
        anyhow::ensure!(
            started.elapsed() <= MIRROR_DEADLINE,
            "timed out waiting for mirror id={id} to reach op={expected_op}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_op_count(
    client: &tokio_postgres::Client,
    mirror: &str,
    operation: i16,
    expected: i64,
) -> Result<()> {
    let started = Instant::now();
    loop {
        let count: i64 = client
            .query_one(
                &format!("SELECT count(*) FROM {mirror} WHERE op = $1"),
                &[&operation],
            )
            .await?
            .get(0);
        if count == expected {
            return Ok(());
        }
        anyhow::ensure!(
            started.elapsed() <= MIRROR_DEADLINE,
            "timed out waiting for {expected} mirror rows with op={operation}; observed {count}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn mirror_row_count(client: &tokio_postgres::Client, mirror: &str, id: i64) -> Result<i64> {
    let row = client
        .query_one(
            &format!("SELECT count(*) FROM {mirror} WHERE id = $1"),
            &[&id],
        )
        .await?;
    Ok(row.get(0))
}

async fn reported_mirror_counter(client: &tokio_postgres::Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(
            &format!(
                r#"
                SELECT COALESCE(m.mirror_row_count, 0)::bigint
                FROM koldstore.manifest m
                WHERE m.table_oid = '{relation}'::regclass
                  AND m.scope_key = ''
                "#
            ),
            &[],
        )
        .await?;
    Ok(row.get(0))
}
