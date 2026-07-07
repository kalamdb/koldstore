//! Shared fixtures and join-plan assertions for koldstore join E2E tests.
#![allow(dead_code)]

use anyhow::{Context, Result};
use tokio_postgres::Client;

use crate::common::{self, ManagedTable, TestDb};

pub const COLD_ROW_COUNT: i64 = 20;

/// Join kinds exercised against mixed hot/cold koldstore tables.
#[derive(Debug, Clone, Copy)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinKind {
    pub fn sql(self) -> &'static str {
        match self {
            Self::Inner => "INNER JOIN",
            Self::Left => "LEFT JOIN",
            Self::Right => "RIGHT JOIN",
            Self::Full => "FULL JOIN",
        }
    }
}

/// Builds a two-table join query selecting both join keys.
#[must_use]
pub fn join_sql(
    join: JoinKind,
    left_relation: &str,
    right_relation: &str,
    left_key: &str,
    right_key: &str,
) -> String {
    format!(
        r#"
        SELECT l.{left_key} AS left_key, r.{right_key} AS right_key
        FROM {left_relation} AS l
        {join} {right_relation} AS r
          ON l.{left_key} = r.{right_key}
        "#,
        join = join.sql()
    )
}

/// Creates a plain PostgreSQL accounts dimension table.
pub async fn create_plain_accounts_table(db: &TestDb, table_name: &str) -> Result<String> {
    let relation = db.relation(table_name);
    db.client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              account_id bigint PRIMARY KEY,
              account_name text NOT NULL
            );
            INSERT INTO {relation} (account_id, account_name)
            SELECT gs, 'account-' || gs::text
            FROM generate_series(0, 16) AS gs;
            INSERT INTO {relation} (account_id, account_name)
            VALUES (50, 'orphan-account'), (99, 'hot-only-account');
            ANALYZE {relation};
            "#
        ))
        .await
        .with_context(|| format!("create plain accounts table {relation}"))?;
    Ok(relation)
}

/// Creates a plain PostgreSQL order-lines table keyed by `item_id`.
pub async fn create_plain_order_lines_table(db: &TestDb, table_name: &str) -> Result<String> {
    let relation = db.relation(table_name);
    db.client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              id bigint PRIMARY KEY,
              item_id bigint NOT NULL,
              line_qty integer NOT NULL
            );
            INSERT INTO {relation} (id, item_id, line_qty)
            SELECT gs, gs, 1
            FROM generate_series(1, {cold_rows}) AS gs;
            ANALYZE {relation};
            "#,
            cold_rows = COLD_ROW_COUNT
        ))
        .await
        .with_context(|| format!("create plain order-lines table {relation}"))?;
    Ok(relation)
}

/// Migrates, flushes, and seeds post-flush hot rows on an indexed items table.
pub async fn setup_koldstore_items_with_mixed_storage(
    db: &TestDb,
    table_name: &str,
) -> Result<ManagedTable> {
    let table = db
        .create_indexed_items_table(table_name, COLD_ROW_COUNT)
        .await?;
    db.migrate_shared(&table.relation, "id").await?;
    db.flush_table(&table.relation).await?;
    common::assert_flush_pruned_hot_storage(&db.client, &table.relation, COLD_ROW_COUNT).await?;

    db.client
        .batch_execute(&format!(
            r#"
            INSERT INTO {relation} (id, account_id, title, qty, category)
            VALUES
              (1001, 5, 'hot-after-flush-1', 1, 'hot'),
              (1002, 99, 'hot-after-flush-2', 2, 'hot');
            ANALYZE {relation};
            "#,
            relation = table.relation
        ))
        .await?;

    let status = common::table_status(&db.client, &table.relation).await?;
    anyhow::ensure!(
        status.hot_rows == 2 && status.cold_row_count >= COLD_ROW_COUNT,
        "expected mixed hot/cold items fixture, got {:?}",
        status
    );
    Ok(table)
}

/// Migrates, flushes, and seeds post-flush hot rows on an order-lines table.
pub async fn setup_koldstore_order_lines_with_mixed_storage(
    db: &TestDb,
    table_name: &str,
) -> Result<ManagedTable> {
    let relation = db.relation(table_name);
    let table = ManagedTable {
        relation: relation.clone(),
        table_name: table_name.to_string(),
        title_index: format!("{table_name}_item_idx"),
    };

    db.client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {relation} (
              id bigint PRIMARY KEY,
              item_id bigint NOT NULL,
              line_qty integer NOT NULL
            );
            CREATE INDEX {item_index} ON {relation} (item_id);
            INSERT INTO {relation} (id, item_id, line_qty)
            SELECT gs, gs, 1
            FROM generate_series(1, {cold_rows}) AS gs;
            ANALYZE {relation};
            "#,
            item_index = table.title_index,
            cold_rows = COLD_ROW_COUNT
        ))
        .await?;

    db.migrate_shared(&relation, "id").await?;
    db.flush_table(&relation).await?;
    common::assert_flush_pruned_hot_storage(&db.client, &relation, COLD_ROW_COUNT).await?;

    db.client
        .batch_execute(&format!(
            r#"
            INSERT INTO {relation} (id, item_id, line_qty)
            VALUES
              (1001, 1001, 10),
              (1002, 3, 20);
            ANALYZE {relation};
            "#,
            relation = relation
        ))
        .await?;

    let status = common::table_status(&db.client, &relation).await?;
    anyhow::ensure!(
        status.hot_rows == 2 && status.cold_row_count >= COLD_ROW_COUNT,
        "expected mixed hot/cold order-lines fixture, got {:?}",
        status
    );
    Ok(table)
}

/// Logs a join plan when `KOLDSTORE_E2E_VERBOSE` is enabled.
pub fn log_join_plan(label: &str, plan: &str) {
    common::log(format!("join plan [{label}]:\n{plan}"));
}

/// Asserts a join plan uses PostgreSQL join execution and KoldMergeScan where expected.
pub fn assert_join_plan_shape(plan: &str, label: &str, min_merge_scans: usize) -> Result<()> {
    anyhow::ensure!(
        plan.contains("Join")
            || plan.contains("Nested Loop")
            || plan.contains("Hash Join")
            || plan.contains("Merge Join"),
        "expected a join node in `{label}` plan, got:\n{plan}"
    );

    let merge_scans = plan.matches("Custom Scan (KoldMergeScan)").count();
    anyhow::ensure!(
        merge_scans >= min_merge_scans,
        "expected at least {min_merge_scans} KoldMergeScan node(s) in `{label}` plan, got {merge_scans}:\n{plan}"
    );
    common::assert_kold_merge_scan_explain(plan)?;
    Ok(())
}

/// Runs `EXPLAIN` and `EXPLAIN ANALYZE` for a join query and asserts cold reads.
pub async fn assert_join_plan_reads_cold_storage(
    client: &Client,
    sql: &str,
    label: &str,
    min_merge_scans: usize,
    min_parquet_segments: usize,
) -> Result<()> {
    let planned = common::explain(client, sql).await?;
    log_join_plan(&format!("{label} EXPLAIN"), &planned);
    assert_join_plan_shape(&planned, label, min_merge_scans)?;
    common::assert_kold_merge_scan_cold_reads(&planned, "manifest.json", min_parquet_segments)?;

    let analyzed = common::explain_analyze(client, sql).await?;
    log_join_plan(&format!("{label} EXPLAIN ANALYZE"), &analyzed);
    common::assert_kold_merge_scan_executed_cold_reads(&analyzed, min_parquet_segments)?;
    Ok(())
}

/// Asserts join row count and that the planner routes koldstore through merge scan.
pub async fn assert_join_pair(
    db: &TestDb,
    join: JoinKind,
    left_relation: &str,
    right_relation: &str,
    left_key: &str,
    right_key: &str,
    expected_rows: i64,
    min_merge_scans: usize,
) -> Result<()> {
    let sql = join_sql(join, left_relation, right_relation, left_key, right_key);
    let count = common::row_count_from_sql(&db.client, &sql).await?;
    anyhow::ensure!(
        count == expected_rows,
        "{join:?} between {left_relation} and {right_relation} expected {expected_rows} rows, got {count}"
    );

    let label = format!("{join:?} {left_relation} x {right_relation}");
    let planned = common::explain(&db.client, &sql).await?;
    log_join_plan(&label, &planned);
    assert_join_plan_shape(&planned, &label, min_merge_scans)?;
    Ok(())
}

pub async fn assert_koldstore_pg_join_samples(
    db: &TestDb,
    items: &str,
    accounts: &str,
) -> Result<()> {
    let cold = db
        .client
        .query_one(
            &format!(
                r#"
                SELECT i.id, i.title, a.account_name
                FROM {items} AS i
                INNER JOIN {accounts} AS a
                  ON i.account_id = a.account_id
                WHERE i.id = 7
                "#,
                items = items,
                accounts = accounts
            ),
            &[],
        )
        .await?;
    assert_eq!(cold.get::<_, i64>(0), 7);
    assert_eq!(cold.get::<_, String>(1), "item-000007");
    assert_eq!(cold.get::<_, String>(2), "account-7");

    let hot = db
        .client
        .query_one(
            &format!(
                r#"
                SELECT i.id, i.title, a.account_name
                FROM {items} AS i
                INNER JOIN {accounts} AS a
                  ON i.account_id = a.account_id
                WHERE i.id = 1001
                "#,
                items = items,
                accounts = accounts
            ),
            &[],
        )
        .await?;
    assert_eq!(hot.get::<_, i64>(0), 1001);
    assert_eq!(hot.get::<_, String>(1), "hot-after-flush-1");
    assert_eq!(hot.get::<_, String>(2), "account-5");

    let sql = format!(
        r#"
        SELECT i.id, a.account_name
        FROM {items} AS i
        INNER JOIN {accounts} AS a
          ON i.account_id = a.account_id
        WHERE i.id IN (1, 1001)
        "#,
        items = items,
        accounts = accounts
    );
    assert_join_plan_reads_cold_storage(&db.client, &sql, "koldstore-pg cold+hot", 1, 1).await?;
    Ok(())
}

pub async fn assert_koldstore_koldstore_join_samples(
    db: &TestDb,
    order_lines: &str,
    items: &str,
) -> Result<()> {
    let cold_cold = db
        .client
        .query_one(
            &format!(
                r#"
                SELECT ol.id, ol.item_id, i.title
                FROM {order_lines} AS ol
                INNER JOIN {items} AS i
                  ON ol.item_id = i.id
                WHERE ol.id = 3
                "#,
                order_lines = order_lines,
                items = items
            ),
            &[],
        )
        .await?;
    assert_eq!(cold_cold.get::<_, i64>(0), 3);
    assert_eq!(cold_cold.get::<_, i64>(1), 3);
    assert_eq!(cold_cold.get::<_, String>(2), "item-000003");

    let hot_hot = db
        .client
        .query_one(
            &format!(
                r#"
                SELECT ol.id, ol.item_id, i.title
                FROM {order_lines} AS ol
                INNER JOIN {items} AS i
                  ON ol.item_id = i.id
                WHERE ol.id = 1001
                "#,
                order_lines = order_lines,
                items = items
            ),
            &[],
        )
        .await?;
    assert_eq!(hot_hot.get::<_, i64>(0), 1001);
    assert_eq!(hot_hot.get::<_, i64>(1), 1001);
    assert_eq!(hot_hot.get::<_, String>(2), "hot-after-flush-1");

    let hot_cold = db
        .client
        .query_one(
            &format!(
                r#"
                SELECT ol.id, ol.item_id, i.title
                FROM {order_lines} AS ol
                INNER JOIN {items} AS i
                  ON ol.item_id = i.id
                WHERE ol.id = 1002
                "#,
                order_lines = order_lines,
                items = items
            ),
            &[],
        )
        .await?;
    assert_eq!(hot_cold.get::<_, i64>(0), 1002);
    assert_eq!(hot_cold.get::<_, i64>(1), 3);
    assert_eq!(hot_cold.get::<_, String>(2), "item-000003");

    let sql = format!(
        r#"
        SELECT ol.id, i.title
        FROM {order_lines} AS ol
        INNER JOIN {items} AS i
          ON ol.item_id = i.id
        WHERE ol.id IN (1, 1001)
        "#,
        order_lines = order_lines,
        items = items
    );
    assert_join_plan_reads_cold_storage(&db.client, &sql, "koldstore-koldstore cold+hot", 1, 1)
        .await?;
    Ok(())
}
