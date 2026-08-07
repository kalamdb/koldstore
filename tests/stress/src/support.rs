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
            e2e::fence_async_mirror(client).await?;
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
    e2e::fence_async_mirror(client).await?;
    let job_id = e2e::flush_table_job_id(client, relation, true)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "flush_table returned NULL for {relation} (force=true); \
                 expected force flush to enqueue work"
            )
        })?;
    let flushed = e2e::wait_for_flush_job_terminal(client, &job_id).await?;
    wait_for_jobs(client, relation).await?;
    Ok(flushed)
}

/// Policy flush (non-force) returning rows_flushed.
///
/// Returns `0` when policy has no due work (`flush_table` returns NULL).
///
/// # Errors
///
/// Returns an error when flush or job lookup fails.
pub async fn flush_table(client: &Client, relation: &str) -> Result<i64> {
    // Policy flush decides from mirror pending counts; catch up WAL apply so
    // recently committed DML is visible to the due check.
    e2e::fence_async_mirror(client).await?;
    let Some(job_id) = e2e::flush_table_job_id(client, relation, false).await? else {
        return Ok(0);
    };
    e2e::wait_for_flush_job_terminal(client, &job_id).await
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
              migration_order_by => $7
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
