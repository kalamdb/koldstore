//! Baseline equality helpers for differential KoldStore vs plain-heap checks.
//!
//! Shared by E2E smoke, isolation, crash-recovery, and storage comparison suites.
//! Comparisons use `EXCEPT ALL` both ways so multiplicity mismatches fail.

use anyhow::{bail, Context, Result};
use tokio_postgres::Client;

/// Asserts that two relations contain identical multisets of rows.
///
/// Compares `SELECT * FROM left` against `SELECT * FROM right` using
/// `EXCEPT ALL` in both directions. Column names and types must be compatible.
///
/// # Errors
///
/// Returns an error when either query fails or the relations differ.
pub async fn assert_relations_equal(client: &Client, left: &str, right: &str) -> Result<()> {
    let left_only = client
        .query(
            &format!(
                r#"
                SELECT * FROM {left}
                EXCEPT ALL
                SELECT * FROM {right}
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("EXCEPT ALL left-only for {left} vs {right}"))?;
    let right_only = client
        .query(
            &format!(
                r#"
                SELECT * FROM {right}
                EXCEPT ALL
                SELECT * FROM {left}
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("EXCEPT ALL right-only for {right} vs {left}"))?;

    if !left_only.is_empty() || !right_only.is_empty() {
        bail!(
            "relations differ: {left} has {} exclusive row(s), {right} has {} exclusive row(s)",
            left_only.len(),
            right_only.len()
        );
    }
    Ok(())
}

/// Asserts that two relations have the same row count.
///
/// # Errors
///
/// Returns an error when counts cannot be read or they disagree.
pub async fn assert_row_counts_equal(client: &Client, left: &str, right: &str) -> Result<()> {
    let left_count = relation_row_count(client, left).await?;
    let right_count = relation_row_count(client, right).await?;
    if left_count != right_count {
        bail!("row count mismatch: {left}={left_count}, {right}={right_count}");
    }
    Ok(())
}

/// Asserts that a relation's primary-key column values are unique.
///
/// # Errors
///
/// Returns an error when duplicates exist or the query fails.
pub async fn assert_pk_unique(client: &Client, relation: &str, pk_columns: &[&str]) -> Result<()> {
    if pk_columns.is_empty() {
        bail!("assert_pk_unique requires at least one PK column");
    }
    let pk_list = pk_columns.join(", ");
    let row = client
        .query_one(
            &format!(
                r#"
                SELECT count(*)::bigint AS dup_groups
                FROM (
                  SELECT {pk_list}, count(*) AS c
                  FROM {relation}
                  GROUP BY {pk_list}
                  HAVING count(*) > 1
                ) dups
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("PK uniqueness check for {relation}"))?;
    let dup_groups: i64 = row.get(0);
    if dup_groups != 0 {
        bail!("{relation} has {dup_groups} duplicate PK group(s) on ({pk_list})");
    }
    Ok(())
}

/// Counts rows in a relation.
///
/// # Errors
///
/// Returns an error when the query fails.
pub async fn relation_row_count(client: &Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(&format!("SELECT count(*)::bigint FROM {relation}"), &[])
        .await
        .with_context(|| format!("count(*) for {relation}"))?;
    Ok(row.get(0))
}
