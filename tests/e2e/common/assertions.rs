//! Reusable E2E assertions.

use anyhow::Result;
use tokio_postgres::Client;

use super::sql::RelationSize;

const INTERNAL_KOLDSTORE_COLUMNS: &[&str] = &["_seq", "_commit_seq", "_deleted", "_user_id"];

/// Asserts an application-visible column list has no KoldStore internal columns.
///
/// # Errors
///
/// Returns an error when any internal column is present.
pub fn assert_no_internal_koldstore_columns(column_names: &[String]) -> Result<()> {
    let found = column_names
        .iter()
        .filter(|column| INTERNAL_KOLDSTORE_COLUMNS.contains(&column.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    anyhow::ensure!(
        found.is_empty(),
        "expected no KoldStore internal columns, found {found:?}"
    );
    Ok(())
}

/// Asserts two ordered SQL identifier lists are identical.
///
/// # Errors
///
/// Returns an error when the actual and expected lists differ.
pub fn assert_ordered_identifiers_eq(actual: &[String], expected: &[String]) -> Result<()> {
    anyhow::ensure!(
        actual == expected,
        "expected ordered identifiers {expected:?}, got {actual:?}"
    );
    Ok(())
}

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

/// Asserts an `EXPLAIN` plan includes `KoldMergeScan`.
pub fn assert_kold_merge_scan_explain(plan: &str) -> Result<()> {
    anyhow::ensure!(
        plan.contains("Custom Scan (KoldMergeScan)"),
        "expected Custom Scan (KoldMergeScan) in plan, got:\n{plan}"
    );
    Ok(())
}

/// Asserts a merge scan `EXPLAIN` lists manifest and parquet cold sources.
pub fn assert_kold_merge_scan_cold_reads(
    plan: &str,
    manifest_path_hint: &str,
    min_parquet_segments: usize,
) -> Result<()> {
    assert_kold_merge_scan_explain(plan)?;
    anyhow::ensure!(
        plan.lines().any(|line| line.contains("Manifest:")),
        "expected manifest read details in plan, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.contains(manifest_path_hint) || plan.contains("manifest.json"),
        "expected manifest path hint `{manifest_path_hint}` in plan, got:\n{plan}"
    );

    let parquet_segments = plan
        .lines()
        .filter(|line| line.contains("Parquet segment:"))
        .count();
    anyhow::ensure!(
        parquet_segments >= min_parquet_segments,
        "expected at least {min_parquet_segments} parquet segment(s), got {parquet_segments} in plan:\n{plan}"
    );
    Ok(())
}

/// Backward-compatible alias for planned cold-read assertions.
pub fn assert_kold_merge_scan_planned_cold_reads(
    plan: &str,
    manifest_path_hint: &str,
    min_parquet_segments: usize,
) -> Result<()> {
    assert_kold_merge_scan_cold_reads(plan, manifest_path_hint, min_parquet_segments)
}

/// Asserts an analyzed merge scan reports timed manifest and parquet reads.
pub fn assert_kold_merge_scan_executed_cold_reads(
    plan: &str,
    min_parquet_segments: usize,
) -> Result<()> {
    assert_kold_merge_scan_explain(plan)?;
    anyhow::ensure!(
        plan.lines()
            .any(|line| line.contains("Manifest:") && line.contains(" ms")),
        "expected timed manifest read in analyzed plan, got:\n{plan}"
    );

    let timed_segments = plan
        .lines()
        .filter(|line| {
            line.contains("Parquet segment:")
                && line.contains(" rows")
                && line.contains(" ms")
                && !line.contains("(planned)")
        })
        .count();
    anyhow::ensure!(
        timed_segments >= min_parquet_segments,
        "expected at least {min_parquet_segments} timed parquet segment(s), got {timed_segments} in plan:\n{plan}"
    );
    anyhow::ensure!(
        plan.lines()
            .any(|line| line.contains("Parquet I/O:") && line.contains("footer-first")),
        "expected footer-first Parquet I/O details in analyzed plan, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.lines().any(|line| line.contains("Row groups:")),
        "expected row-group prune details in analyzed plan, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.lines().any(|line| line.contains("Bloom:")),
        "expected bloom prune details in analyzed plan, got:\n{plan}"
    );
    anyhow::ensure!(
        plan.lines()
            .any(|line| line.contains("Manifest:") && line.contains("source=catalog")),
        "expected catalog manifest source in analyzed plan, got:\n{plan}"
    );
    Ok(())
}

/// Backward-compatible alias for older tests.
pub fn assert_merge_scan_explain(plan: &str) -> Result<()> {
    assert_kold_merge_scan_explain(plan)
}

/// Asserts hot DML instrumentation did not record object-store reads.
pub fn assert_no_object_store_reads(counter_value: i64) -> Result<()> {
    anyhow::ensure!(
        counter_value == 0,
        "expected no object-store reads on hot DML path, got {counter_value}"
    );
    Ok(())
}

/// Asserts pg-koldstore system columns do not create dramatic heap bloat.
pub fn assert_system_column_size_overhead(
    baseline: RelationSize,
    managed: RelationSize,
    rows: i64,
) -> Result<()> {
    let heap_overhead_per_row = managed.heap_overhead_per_row(baseline, rows);
    anyhow::ensure!(
        heap_overhead_per_row <= 160,
        "expected system-column heap overhead <= 160 bytes/row, got {heap_overhead_per_row}; baseline={baseline:?}, managed={managed:?}"
    );

    let index_overhead_per_row =
        managed.indexes_bytes.saturating_sub(baseline.indexes_bytes) / rows.max(1);
    anyhow::ensure!(
        index_overhead_per_row <= 96,
        "expected index overhead <= 96 bytes/row, got {index_overhead_per_row}; baseline={baseline:?}, managed={managed:?}"
    );

    Ok(())
}

/// Asserts a catalog index plan is index-backed.
pub fn assert_catalog_index_plan(plan: &str, index_name: &str) -> Result<()> {
    anyhow::ensure!(
        plan.contains(index_name),
        "expected {index_name} in plan, got:\n{plan}"
    );
    assert_catalog_plan_is_index_backed(plan)
}

/// Asserts a catalog plan uses at least one acceptable index.
pub fn assert_catalog_index_plan_uses_any(plan: &str, index_names: &[&str]) -> Result<()> {
    anyhow::ensure!(
        index_names
            .iter()
            .any(|index_name| plan.contains(index_name)),
        "expected one of {} in plan, got:\n{plan}",
        index_names.join(", ")
    );
    assert_catalog_plan_is_index_backed(plan)
}

fn assert_catalog_plan_is_index_backed(plan: &str) -> Result<()> {
    anyhow::ensure!(
        plan.contains("Index Scan")
            || plan.contains("Index Only Scan")
            || plan.contains("Bitmap Index Scan"),
        "expected index-backed catalog plan, got:\n{plan}"
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
