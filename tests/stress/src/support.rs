//! Thin wrappers around shared e2e helpers used by the stress harness.

use anyhow::Result;
use tokio_postgres::Client;

use crate::e2e;

/// Sets `koldstore.user_id` for a tenant-scoped session.
///
/// # Errors
///
/// Returns an error when the GUC cannot be set.
pub async fn set_scope(client: &Client, scope_id: &str) -> Result<()> {
    client
        .batch_execute(&format!(
            "SET koldstore.user_id = '{}'",
            scope_id.replace('\'', "''")
        ))
        .await?;
    Ok(())
}

/// Waits until no active jobs remain for a managed table, then fences async mirror.
///
/// # Errors
///
/// Returns an error when job polling fails or the timeout elapses.
pub async fn wait_for_jobs(client: &Client, relation: &str) -> Result<()> {
    for _ in 0..180 {
        let active = e2e::active_job_count(client, relation).await?;
        if active == 0 {
            e2e::fence_selected_mirror(client).await?;
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    anyhow::bail!("timed out waiting for jobs on {relation}");
}

/// Force-flushes a managed table and returns rows_flushed from the job row.
///
/// # Errors
///
/// Returns an error when enqueue/flush/job lookup fails or the job failed.
pub async fn force_flush_table(client: &Client, relation: &str) -> Result<i64> {
    wait_for_jobs(client, relation).await?;
    let row = client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass, true)::text",
            &[&relation],
        )
        .await?;
    let job_id: String = row.get(0);
    let progress = client
        .query_one(
            r#"
            SELECT
              COALESCE(rows_flushed, 0)::bigint,
              COALESCE(status::text, ''),
              COALESCE(error_trace, '')
            FROM koldstore.jobs
            WHERE id = $1::text::uuid
            "#,
            &[&job_id],
        )
        .await?;
    let flushed: i64 = progress.get(0);
    let status: String = progress.get(1);
    let error: String = progress.get(2);
    wait_for_jobs(client, relation).await?;
    if status.eq_ignore_ascii_case("failed") || !error.is_empty() {
        anyhow::bail!("force flush of {relation} failed status={status} error={error}");
    }
    Ok(flushed)
}

/// Policy flush (non-force) returning rows_flushed.
///
/// # Errors
///
/// Returns an error when flush or job lookup fails.
pub async fn flush_table(client: &Client, relation: &str) -> Result<i64> {
    let row = client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass)::text",
            &[&relation],
        )
        .await?;
    let job_id: String = row.get(0);
    let progress = client
        .query_one(
            "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        )
        .await?;
    Ok(progress.get(0))
}

/// Registers a user-scoped managed table with aggressive small-file flush policy.
///
/// # Errors
///
/// Returns an error when manage_table or catalog assertions fail.
#[allow(clippy::too_many_arguments)]
pub async fn manage_user_scoped_with_policy(
    client: &Client,
    storage_name: &str,
    relation: &str,
    scope_column: &str,
    migration_order_by: &str,
    hot_row_limit: i64,
    min_flush_rows: i64,
    max_rows_per_file: i64,
) -> Result<()> {
    let mode = e2e::selected_mirror_capture_mode()?.as_str();
    client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name        => $1::text::regclass,
              storage           => $2,
              hot_row_limit     => $3,
              min_flush_rows    => $4,
              max_rows_per_file => $5,
              table_type        => 'user',
              scope_column      => $6,
              migration_order_by => $7,
              mirror_capture_mode => $8
            )
            "#,
            &[
                &relation,
                &storage_name,
                &hot_row_limit,
                &min_flush_rows,
                &max_rows_per_file,
                &scope_column,
                &migration_order_by,
                &mode,
            ],
        )
        .await?;
    e2e::assert_system_columns_absent(client, relation).await?;
    e2e::assert_catalog_has_active_schema(client, relation).await?;
    Ok(())
}

pub fn log_always(message: impl AsRef<str>) {
    e2e::log_always(format!("[stress] {}", message.as_ref()));
}
