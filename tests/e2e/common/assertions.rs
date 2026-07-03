//! Reusable E2E assertions.

use anyhow::Result;
use tokio_postgres::Client;

/// Asserts no duplicate hot rows exist for a managed table primary key expression.
///
/// # Errors
///
/// Returns an error when the SQL query fails or duplicates exist.
pub async fn assert_no_duplicate_hot_pk(
    client: &Client,
    table_name: &str,
    pk_expr: &str,
) -> Result<()> {
    let sql = format!(
        "SELECT count(*) FROM (SELECT {pk_expr}, count(*) FROM {table_name} GROUP BY {pk_expr} HAVING count(*) > 1) d"
    );
    let row = client.query_one(&sql, &[]).await?;
    let duplicate_groups: i64 = row.get(0);
    anyhow::ensure!(duplicate_groups == 0, "duplicate hot PK rows found");
    Ok(())
}

/// Asserts an EXPLAIN plan includes KoldstoreMergeScan.
pub fn assert_merge_scan_explain(plan: &str) -> Result<()> {
    anyhow::ensure!(
        plan.contains("KoldstoreMergeScan"),
        "expected KoldstoreMergeScan in plan"
    );
    Ok(())
}

/// Asserts hot DML instrumentation did not record object-store reads.
pub fn assert_no_object_store_reads(counter_value: i64) -> Result<()> {
    anyhow::ensure!(
        counter_value == 0,
        "expected no object-store reads on hot DML path, got {counter_value}"
    );
    Ok(())
}

/// Asserts an object path appears in a MinIO listing captured by a test.
pub fn assert_minio_listing_contains(listing: &str, expected_path: &str) -> Result<()> {
    anyhow::ensure!(
        listing.lines().any(|line| line.contains(expected_path)),
        "expected MinIO listing to contain {expected_path}"
    );
    Ok(())
}
