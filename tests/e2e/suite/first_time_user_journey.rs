//! First-time user journey: multi-table manage, second DB, unmanage sibling.
//!
//! Mimics someone trying KoldStore for the first time across one database with
//! two managed tables and (when the E2E DB pool has ≥2 workers) a second
//! database in parallel. Pieces of this exist elsewhere (`demigrate_matrix`,
//! `flush_multiple_tables_in_parallel_*`, `multi_database_stress`); this test
//! stitches the quickstart-shaped path into one regression.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use tokio::task::JoinHandle;

use crate::common;

const ORDERS_ROWS: i64 = 120;
const MESSAGES_ROWS: i64 = 80;
const INVOICES_ROWS: i64 = 60;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_time_user_multi_table_unmanage_and_optional_second_database() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let primary = common::TestDb::start(target.clone(), "ftu_primary").await?;
        let orders = primary.relation("orders");
        let messages = primary.relation("messages");

        primary
            .client
            .batch_execute(&format!(
                "CREATE TABLE {orders} (
                   id bigint PRIMARY KEY,
                   customer text NOT NULL,
                   amount_cents bigint NOT NULL
                 );
                 CREATE TABLE {messages} (
                   id bigint PRIMARY KEY,
                   account_id bigint NOT NULL,
                   body text NOT NULL
                 );"
            ))
            .await
            .context("create primary app tables")?;

        // Separate statements so slot provisioning is not after uncommitted DDL.
        manage_table(&primary, &orders, 50).await?;
        manage_table(&primary, &messages, 50).await?;
        common::wait_for_async_worker(&primary.client).await?;

        let mut second_db: Option<(common::TestDb, String)> = None;
        if common::e2e_db_pool_enabled() && common::e2e_pool_size() >= 2 {
            let secondary = common::TestDb::start(target.clone(), "ftu_secondary").await?;
            let invoices = secondary.relation("invoices");
            secondary
                .client
                .batch_execute(&format!(
                    "CREATE TABLE {invoices} (
                       id bigint PRIMARY KEY,
                       total_cents bigint NOT NULL,
                       note text NOT NULL
                     );"
                ))
                .await
                .context("create secondary invoices table")?;
            manage_table(&secondary, &invoices, 40).await?;
            common::wait_for_async_worker(&secondary.client).await?;
            second_db = Some((secondary, invoices));
        }

        // Parallel inserts across tables / databases (first-time multi-tenant feel).
        let orders_insert = spawn_insert(
            &primary,
            format!(
                "INSERT INTO {orders}
                 SELECT g, 'cust-' || g, g * 100
                 FROM generate_series(1, {ORDERS_ROWS}) g"
            ),
        );
        let messages_insert = spawn_insert(
            &primary,
            format!(
                "INSERT INTO {messages}
                 SELECT g, g % 7, 'msg-' || g
                 FROM generate_series(1, {MESSAGES_ROWS}) g"
            ),
        );
        let invoices_insert = if let Some((ref secondary, ref invoices)) = second_db {
            Some(spawn_insert(
                secondary,
                format!(
                    "INSERT INTO {invoices}
                     SELECT g, g * 1000, 'inv-' || g
                     FROM generate_series(1, {INVOICES_ROWS}) g"
                ),
            ))
        } else {
            None
        };

        orders_insert.await??;
        messages_insert.await??;
        if let Some(handle) = invoices_insert {
            handle.await??;
        }

        common::fence_async_mirror(&primary.client).await?;
        if let Some((ref secondary, _)) = second_db {
            common::fence_async_mirror(&secondary.client).await?;
        }

        anyhow::ensure!(common::row_count(&primary.client, &orders).await? == ORDERS_ROWS);
        anyhow::ensure!(common::row_count(&primary.client, &messages).await? == MESSAGES_ROWS);
        if let Some((ref secondary, ref invoices)) = second_db {
            anyhow::ensure!(common::row_count(&secondary.client, invoices).await? == INVOICES_ROWS);
        }

        let orders_feed = drain_changes_since(&primary.client, &orders, 0, 40).await?;
        anyhow::ensure!(
            orders_feed.len() as i64 == ORDERS_ROWS,
            "orders changes_since expected {ORDERS_ROWS}, got {}",
            orders_feed.len()
        );
        assert_exclusive_seq(&orders_feed)?;
        let messages_feed = drain_changes_since(&primary.client, &messages, 0, 40).await?;
        anyhow::ensure!(messages_feed.len() as i64 == MESSAGES_ROWS);
        assert_exclusive_seq(&messages_feed)?;
        if let Some((ref secondary, ref invoices)) = second_db {
            let invoices_feed = drain_changes_since(&secondary.client, invoices, 0, 40).await?;
            anyhow::ensure!(invoices_feed.len() as i64 == INVOICES_ROWS);
            assert_exclusive_seq(&invoices_feed)?;
        }

        let flushed_orders = primary.flush_table_with_force(&orders, true).await?;
        let flushed_messages = primary.flush_table_with_force(&messages, true).await?;
        anyhow::ensure!(flushed_orders > 0 && flushed_messages > 0);
        anyhow::ensure!(common::row_count(&primary.client, &orders).await? == ORDERS_ROWS);
        anyhow::ensure!(common::row_count(&primary.client, &messages).await? == MESSAGES_ROWS);
        if let Some((ref secondary, ref invoices)) = second_db {
            let flushed = secondary.flush_table_with_force(invoices, true).await?;
            anyhow::ensure!(flushed > 0);
            anyhow::ensure!(common::row_count(&secondary.client, invoices).await? == INVOICES_ROWS);
        }

        // Unmanage one sibling; the other must keep accepting DML + changes_since.
        let deactivated: i64 = primary
            .client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&messages],
            )
            .await?
            .get(0);
        anyhow::ensure!(deactivated == 1);
        let messages_active: i64 = primary
            .client
            .query_one(
                "SELECT count(*) FROM koldstore.schemas \
                 WHERE table_oid = $1::text::regclass::oid AND active",
                &[&messages],
            )
            .await?
            .get(0);
        let orders_active: i64 = primary
            .client
            .query_one(
                "SELECT count(*) FROM koldstore.schemas \
                 WHERE table_oid = $1::text::regclass::oid AND active",
                &[&orders],
            )
            .await?
            .get(0);
        anyhow::ensure!(messages_active == 0 && orders_active == 1);
        anyhow::ensure!(common::row_count(&primary.client, &messages).await? == MESSAGES_ROWS);

        let cursor_before = tip_seq(&primary.client, &orders).await?;
        primary
            .client
            .execute(
                &format!("INSERT INTO {orders} VALUES ($1, $2, $3)"),
                &[&(ORDERS_ROWS + 1), &"after-unmanage", &1_i64],
            )
            .await?;
        common::fence_async_mirror(&primary.client).await?;
        let after = drain_changes_since(&primary.client, &orders, cursor_before, 20).await?;
        let ids: BTreeSet<i64> = after.iter().map(|(_, id, _)| *id).collect();
        anyhow::ensure!(
            ids.contains(&(ORDERS_ROWS + 1)),
            "orders changes_since must show post-unmanage insert; got {ids:?}"
        );
        anyhow::ensure!(common::row_count(&primary.client, &orders).await? == ORDERS_ROWS + 1);

        if let Some((ref secondary, ref invoices)) = second_db {
            let cursor = tip_seq(&secondary.client, invoices).await?;
            secondary
                .client
                .execute(
                    &format!("INSERT INTO {invoices} VALUES ($1, $2, $3)"),
                    &[&(INVOICES_ROWS + 1), &1_i64, &"still-ok"],
                )
                .await?;
            common::fence_async_mirror(&secondary.client).await?;
            let page = drain_changes_since(&secondary.client, invoices, cursor, 20).await?;
            let ids: BTreeSet<i64> = page.iter().map(|(_, id, _)| *id).collect();
            anyhow::ensure!(ids.contains(&(INVOICES_ROWS + 1)));
            anyhow::ensure!(
                common::row_count(&secondary.client, invoices).await? == INVOICES_ROWS + 1
            );
            let healthy: bool = secondary
                .client
                .query_one(
                    "SELECT (koldstore.async_mirror_status()->>'healthy')::boolean",
                    &[],
                )
                .await?
                .get(0);
            anyhow::ensure!(healthy, "secondary database async mirror must stay healthy");
        }
    }

    Ok(())
}

async fn manage_table(db: &common::TestDb, relation: &str, hot_row_limit: i64) -> Result<()> {
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => $3::bigint,
              min_flush_rows => 1,
              max_rows_per_file => 1000,
              auto_flush => false
            )
            "#,
            &[&relation, &db.storage_name, &hot_row_limit],
        )
        .await
        .with_context(|| format!("manage_table {relation}"))?;
    common::assert_catalog_has_active_schema(&db.client, relation).await?;
    Ok(())
}

fn spawn_insert(db: &common::TestDb, sql: String) -> JoinHandle<Result<()>> {
    let conninfo = db.target.connection_string();
    tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
            .await
            .context("peer connect for insert")?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&sql)
            .await
            .context("parallel insert")?;
        Ok(())
    })
}

async fn tip_seq(client: &tokio_postgres::Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(
            "SELECT COALESCE(max(seq), 0)::bigint \
             FROM koldstore.changes_since($1::text::regclass, 0, 1, 1)",
            &[&relation],
        )
        .await?;
    Ok(row.get(0))
}

async fn drain_changes_since(
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
            .await
            .with_context(|| format!("changes_since page for {relation} since={cursor}"))?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let seq: i64 = row.get(0);
            anyhow::ensure!(
                seq > cursor,
                "exclusive cursor must advance: got {seq} after {cursor}"
            );
            cursor = seq;
            out.push((seq, row.get(1), row.get(2)));
        }
    }
    Ok(out)
}

fn assert_exclusive_seq(rows: &[(i64, i64, String)]) -> Result<()> {
    let mut last = 0_i64;
    let mut seen_ids = BTreeSet::new();
    for (seq, id, _) in rows {
        anyhow::ensure!(*seq > last, "seq must be strictly increasing");
        anyhow::ensure!(
            seen_ids.insert(*id),
            "duplicate id {id} in changes_since drain"
        );
        last = *seq;
    }
    Ok(())
}
