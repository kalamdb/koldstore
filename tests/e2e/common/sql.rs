//! SQL helpers used by pgrx-backed E2E tests.

use anyhow::Result;
use tokio_postgres::Client;

/// PostgreSQL relation size snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationSize {
    /// Heap and TOAST bytes, excluding indexes.
    pub table_bytes: i64,
    /// Index bytes.
    pub indexes_bytes: i64,
    /// Total relation bytes.
    pub total_bytes: i64,
}

impl RelationSize {
    /// Extra heap bytes per row versus another relation.
    #[must_use]
    pub fn heap_overhead_per_row(self, baseline: Self, rows: i64) -> i64 {
        if rows == 0 {
            return 0;
        }
        self.table_bytes.saturating_sub(baseline.table_bytes) / rows
    }
}

/// Returns a relation size snapshot.
///
/// # Errors
///
/// Returns an error when PostgreSQL rejects the relation name.
pub async fn relation_size(client: &Client, relation: &str) -> Result<RelationSize> {
    let row = client
        .query_one(
            r#"
            SELECT
              pg_table_size($1::text::regclass)::bigint,
              pg_indexes_size($1::text::regclass)::bigint,
              pg_total_relation_size($1::text::regclass)::bigint
            "#,
            &[&relation],
        )
        .await?;

    Ok(RelationSize {
        table_bytes: row.get(0),
        indexes_bytes: row.get(1),
        total_bytes: row.get(2),
    })
}

/// Counts rows in a relation.
///
/// # Errors
///
/// Returns an error when the query fails.
pub async fn row_count(client: &Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
        .await?;
    Ok(row.get(0))
}

/// Returns an `EXPLAIN (COSTS OFF)` plan as text.
///
/// # Errors
///
/// Returns an error when `EXPLAIN` fails.
pub async fn explain(client: &Client, sql: &str) -> Result<String> {
    let rows = client
        .query(&format!("EXPLAIN (COSTS OFF) {sql}"), &[])
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// Returns an `EXPLAIN` plan with sequential scans disabled for index eligibility checks.
///
/// # Errors
///
/// Returns an error when PostgreSQL rejects the statement.
pub async fn explain_with_seqscan_disabled(client: &Client, sql: &str) -> Result<String> {
    client.batch_execute("SET enable_seqscan = off").await?;
    let plan = explain(client, sql).await;
    client.batch_execute("SET enable_seqscan = on").await?;
    plan
}

/// Asserts that an `EXPLAIN` plan uses an expected index.
///
/// # Errors
///
/// Returns an error when the plan does not include an index scan or index name.
pub fn assert_index_scan(plan: &str, index_name: &str) -> Result<()> {
    anyhow::ensure!(
        plan.contains("Index Scan")
            || plan.contains("Index Only Scan")
            || plan.contains("Bitmap Index Scan"),
        "expected an index-backed plan, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.contains(index_name),
        "expected plan to use {index_name}, got:\n{plan}"
    );
    Ok(())
}
