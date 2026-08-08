//! Reference-model oracle for managed vs plain-heap equality checks.
//!
//! E2E crash / recovery paths clone an unmodified PostgreSQL reference table
//! alongside a managed relation, apply identical DML to both, then assert
//! multiset equality with `EXCEPT ALL` (no client-side merge).

use anyhow::{bail, Context, Result};
use tokio_postgres::Client;

use super::equality::{assert_pk_unique, assert_relations_equal, assert_row_counts_equal};

/// Builds `CREATE TABLE … AS SELECT` SQL that clones `source` into `reference`.
///
/// Exposed for unit tests that assert the SQL shape without a live server.
#[must_use]
pub fn clone_reference_sql(source: &str, reference: &str, pk_columns: &[&str]) -> String {
    let pk = if pk_columns.is_empty() {
        String::new()
    } else {
        format!(
            "ALTER TABLE {reference} ADD PRIMARY KEY ({});",
            pk_columns.join(", ")
        )
    };
    format!("CREATE TABLE {reference} AS SELECT * FROM {source};\n{pk}")
}

/// Creates a plain-heap reference clone of `managed` (same rows, optional PK).
///
/// # Errors
///
/// Returns an error when DDL fails.
pub async fn create_reference_clone(
    client: &Client,
    managed: &str,
    reference: &str,
    pk_columns: &[&str],
) -> Result<()> {
    client
        .batch_execute(&clone_reference_sql(managed, reference, pk_columns))
        .await
        .with_context(|| format!("create reference clone {reference} from {managed}"))?;
    Ok(())
}

/// Asserts managed and reference relations contain the same multiset of rows.
///
/// Uses bidirectional `EXCEPT ALL` plus a row-count check.
///
/// # Errors
///
/// Returns an error when the relations differ or queries fail.
pub async fn assert_managed_matches_reference(
    client: &Client,
    managed: &str,
    reference: &str,
) -> Result<()> {
    assert_relations_equal(client, reference, managed)
        .await
        .with_context(|| format!("oracle EXCEPT ALL for {managed} vs {reference}"))?;
    assert_row_counts_equal(client, reference, managed)
        .await
        .with_context(|| format!("oracle row count for {managed} vs {reference}"))?;
    Ok(())
}

/// Asserts logical table equality, PK uniqueness, and KoldStore catalog/object integrity.
///
/// This is the preferred quiescent-state oracle for managed-table E2E tests. It
/// deliberately combines three independent checks:
///
/// - differential equality against a plain PostgreSQL heap using `EXCEPT ALL`;
/// - uniqueness of the logical primary key after hot/cold winner resolution;
/// - `koldstore.verify_table_integrity`, which validates KoldStore metadata and
///   published cold-storage invariants.
///
/// A row-count-only assertion can miss wrong values, duplicates that happen to
/// balance missing rows, or catalog/object divergence, so lifecycle tests should
/// use this helper whenever a reference table is available.
///
/// # Errors
///
/// Returns an error when any independent integrity oracle fails.
pub async fn assert_managed_integrity(
    client: &Client,
    managed: &str,
    reference: &str,
    pk_columns: &[&str],
) -> Result<()> {
    assert_managed_matches_reference(client, managed, reference).await?;
    assert_pk_unique(client, managed, pk_columns).await?;

    let integrity_text: String = client
        .query_one(
            "SELECT koldstore.verify_table_integrity($1::text::regclass)::text",
            &[&managed],
        )
        .await
        .with_context(|| format!("verify_table_integrity for {managed}"))?
        .get(0);
    let integrity: serde_json::Value = serde_json::from_str(&integrity_text)
        .with_context(|| format!("parse verify_table_integrity result for {managed}"))?;
    if integrity.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        bail!("verify_table_integrity reported failure for {managed}: {integrity}");
    }
    Ok(())
}

/// Runs the same DML statement against both relations.
///
/// `sql_template` must contain the `{rel}` placeholder (once or more).
///
/// # Errors
///
/// Returns an error when either execution fails.
pub async fn apply_dml_to_both(
    client: &Client,
    managed: &str,
    reference: &str,
    sql_template: &str,
) -> Result<()> {
    if !sql_template.contains("{rel}") {
        bail!("apply_dml_to_both template must contain {{rel}} placeholder");
    }
    for relation in [reference, managed] {
        let sql = sql_template.replace("{rel}", relation);
        client
            .batch_execute(&sql)
            .await
            .with_context(|| format!("oracle DML on {relation}"))?;
    }
    Ok(())
}

/// Projection equality via `EXCEPT ALL` on explicit columns.
///
/// Prefer [`assert_managed_matches_reference`] when `SELECT *` column layouts match.
/// `order_by_cols` names the compared columns (typically the PK / business key set).
/// Despite the historical function name, ordering is not part of this check;
/// multiset equality is intentionally order-independent.
///
/// # Errors
///
/// Returns an error when projections differ or queries fail.
pub async fn assert_managed_matches_reference_ordered(
    client: &Client,
    managed: &str,
    reference: &str,
    order_by_cols: &[&str],
) -> Result<()> {
    if order_by_cols.is_empty() {
        bail!("assert_managed_matches_reference_ordered requires order_by_cols");
    }
    let cols = order_by_cols.join(", ");
    let left_only = client
        .query(
            &format!(
                r#"
                SELECT {cols} FROM {reference}
                EXCEPT ALL
                SELECT {cols} FROM {managed}
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("ordered EXCEPT left-only {reference} vs {managed}"))?;
    let right_only = client
        .query(
            &format!(
                r#"
                SELECT {cols} FROM {managed}
                EXCEPT ALL
                SELECT {cols} FROM {reference}
                "#
            ),
            &[],
        )
        .await
        .with_context(|| format!("ordered EXCEPT right-only {managed} vs {reference}"))?;
    if !left_only.is_empty() || !right_only.is_empty() {
        bail!(
            "ordered oracle mismatch: {reference} exclusive={}, {managed} exclusive={}",
            left_only.len(),
            right_only.len()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::clone_reference_sql;

    #[test]
    fn clone_reference_sql_includes_select_star_and_pk() {
        let sql = clone_reference_sql("s.managed", "s.ref", &["id"]);
        assert!(sql.contains("CREATE TABLE s.ref AS SELECT * FROM s.managed"));
        assert!(sql.contains("ALTER TABLE s.ref ADD PRIMARY KEY (id)"));
    }

    #[test]
    fn clone_reference_sql_omits_pk_when_empty() {
        let sql = clone_reference_sql("a", "b", &[]);
        assert!(sql.contains("CREATE TABLE b AS SELECT * FROM a"));
        assert!(!sql.contains("PRIMARY KEY"));
    }
}
