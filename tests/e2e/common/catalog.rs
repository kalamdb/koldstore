//! Catalog assertions and helpers for managed-table E2E tests.

use anyhow::Result;
use tokio_postgres::Client;

/// Asserts that pg-koldstore system columns are present on a relation.
///
/// # Errors
///
/// Returns an error when the catalog query fails or any column is missing.
pub async fn assert_system_columns_present(client: &Client, relation: &str) -> Result<()> {
    let row = client
        .query_one(
            r#"
            SELECT count(*)
            FROM pg_attribute
            WHERE attrelid = $1::text::regclass
              AND attname = ANY($2)
              AND NOT attisdropped
            "#,
            &[&relation, &&["_seq", "_commit_seq", "_deleted"][..]],
        )
        .await?;
    anyhow::ensure!(
        row.get::<_, i64>(0) == 3,
        "expected _seq, _commit_seq, and _deleted on {relation}"
    );
    Ok(())
}

/// Asserts that a relation has one active pg-koldstore schema row.
///
/// # Errors
///
/// Returns an error when the catalog query fails or the schema is inactive.
pub async fn assert_catalog_has_active_schema(client: &Client, relation: &str) -> Result<()> {
    let row = client
        .query_one(
            r#"
            SELECT count(*)
            FROM koldstore.schemas
            WHERE table_oid = $1::text::regclass::oid
              AND active
            "#,
            &[&relation],
        )
        .await?;
    anyhow::ensure!(
        row.get::<_, i64>(0) == 1,
        "expected one active schema row for {relation}"
    );
    Ok(())
}

/// Counts active jobs for a relation.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub async fn active_job_count(client: &Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(
            r#"
            SELECT count(*)
            FROM koldstore.jobs
            WHERE table_oid = $1::text::regclass::oid
              AND status IN ('pending', 'running')
            "#,
            &[&relation],
        )
        .await?;
    Ok(row.get(0))
}

/// Asserts that no pending or running jobs remain for a relation.
///
/// # Errors
///
/// Returns an error when the catalog query fails or jobs remain active.
pub async fn assert_no_active_jobs(client: &Client, relation: &str) -> Result<()> {
    let active = active_job_count(client, relation).await?;
    anyhow::ensure!(
        active == 0,
        "expected no active jobs for {relation}, got {active}"
    );
    Ok(())
}

/// Counts active cold segments for a relation.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub async fn cold_segment_count(client: &Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(
            r#"
            SELECT count(*)
            FROM koldstore.cold_segments
            WHERE table_oid = $1::text::regclass::oid
              AND status = 'active'
            "#,
            &[&relation],
        )
        .await?;
    Ok(row.get(0))
}

/// Counts manifest rows for a relation.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub async fn manifest_count(client: &Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(
            r#"
            SELECT count(*)
            FROM koldstore.manifest
            WHERE table_oid = $1::text::regclass::oid
            "#,
            &[&relation],
        )
        .await?;
    Ok(row.get(0))
}

/// Asserts that active cold metadata exists for a flushed relation.
///
/// # Errors
///
/// Returns an error when cold segment or PK hint metadata is absent.
pub async fn assert_cold_metadata_present(client: &Client, relation: &str) -> Result<()> {
    let row = client
        .query_one(
            r#"
            SELECT
              count(DISTINCT cs.segment_id),
              count(DISTINCT h.pk_hash),
              COALESCE(sum(cs.byte_size), 0)::bigint
            FROM koldstore.cold_segments cs
            LEFT JOIN koldstore.cold_pk_hints h
              ON h.table_oid = cs.table_oid
             AND h.scope_key = cs.scope_key
             AND h.segment_id = cs.segment_id
            WHERE cs.table_oid = $1::text::regclass::oid
              AND cs.status = 'active'
            "#,
            &[&relation],
        )
        .await?;
    anyhow::ensure!(
        row.get::<_, i64>(0) > 0,
        "expected active cold segment metadata for {relation}"
    );
    anyhow::ensure!(
        row.get::<_, i64>(1) > 0,
        "expected cold PK hint metadata for {relation}"
    );
    anyhow::ensure!(
        row.get::<_, i64>(2) > 0,
        "expected positive cold segment byte size for {relation}"
    );
    Ok(())
}
