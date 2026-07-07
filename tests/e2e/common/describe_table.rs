//! Managed-table storage status helpers for E2E verification.

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio_postgres::Client;

use super::catalog::change_log_mirror_relation;
use super::sql::{hot_row_count, row_count};

/// Storage and flush status returned by `koldstore.describe_table`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TableStorageStatus {
    pub hot_rows: i64,
    pub mirror_rows: i64,
    pub cold_row_count: i64,
    pub cold_segment_count: i64,
    pub heap_size_bytes: i64,
    pub table_size_bytes: i64,
    pub index_size_bytes: i64,
    pub manifest_state: Option<String>,
    pub manifest_max_seq: i64,
    pub pending_jobs: i64,
    pub storage_binding: Option<String>,
    pub last_error: Option<String>,
}

/// Loads managed-table storage status through `koldstore.describe_table`.
///
/// # Errors
///
/// Returns an error when the SQL function is unavailable or the payload is invalid.
pub async fn describe_table(client: &Client, relation: &str) -> Result<TableStorageStatus> {
    let row = client
        .query_one(
            "SELECT koldstore.describe_table(table_name => $1::text::regclass)::text",
            &[&relation],
        )
        .await
        .with_context(|| format!("load describe_table for {relation}"))?;
    let value: serde_json::Value = serde_json::from_str(&row.get::<_, String>(0))
        .with_context(|| format!("decode describe_table JSON text for {relation}"))?;
    serde_json::from_value(value)
        .with_context(|| format!("decode describe_table payload for {relation}"))
}

/// Asserts that a full flush moved live rows to cold storage and pruned hot/mirror rows.
///
/// Verifies both the managed `describe_table` payload and direct row counts on the base
/// table and its `__cl` mirror so flush cleanup cannot regress on only one side.
///
/// # Errors
///
/// Returns an error when cold accounting is wrong or hot/mirror rows were not pruned.
pub async fn assert_flush_pruned_hot_storage(
    client: &Client,
    relation: &str,
    expected_cold_rows: i64,
) -> Result<()> {
    let mirror = change_log_mirror_relation(relation);
    let status = describe_table(client, relation).await?;
    let hot_rows = hot_row_count(client, relation).await?;
    let mirror_rows = row_count(client, &mirror).await?;

    anyhow::ensure!(
        status.cold_row_count >= expected_cold_rows,
        "expected at least {expected_cold_rows} cold rows for {relation}, got {:?}",
        status
    );
    anyhow::ensure!(
        status.hot_rows == 0,
        "expected flushed live rows to be pruned from hot heap for {relation}, got {:?}",
        status
    );
    anyhow::ensure!(
        status.mirror_rows == 0,
        "expected flushed mirror rows to be cleaned for {relation}, got {:?}",
        status
    );
    anyhow::ensure!(
        hot_rows == 0,
        "expected hot heap for {relation} to be empty after flush cleanup, got {hot_rows} rows (mirror={mirror_rows})"
    );
    anyhow::ensure!(
        mirror_rows == 0,
        "expected change-log mirror {mirror} to be empty after flush cleanup, got {mirror_rows} rows (base={hot_rows})"
    );
    Ok(())
}

/// Asserts that cold storage grew while hot rows remain until runtime prune lands.
///
/// # Errors
///
/// Returns an error when cold accounting does not match expectations.
pub async fn assert_cold_rows_at_least(
    client: &Client,
    relation: &str,
    expected_cold_rows: i64,
) -> Result<TableStorageStatus> {
    let status = describe_table(client, relation).await?;
    anyhow::ensure!(
        status.cold_row_count >= expected_cold_rows,
        "expected at least {expected_cold_rows} cold rows for {relation}, got {:?}",
        status
    );
    Ok(status)
}
