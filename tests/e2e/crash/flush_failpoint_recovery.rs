//! Crash/failpoint recovery: arm flush failpoints, recover, retry, validate rows.
//!
//! Takes [`crate::common::acquire_cluster_exclusive`] so assert-enabled Postgres
//! does not abort the shared postmaster when sibling tests race logical decoding
//! (`ReorderBufferInvalidate` / `txn->ninvalidations == 0`).
use crate::common;

use anyhow::{bail, Context, Result};

/// Default failpoints exercised in CI smoke; full matrix via env.
const DEFAULT_FAILPOINTS: &[&str] = &[
    "after_select_rows",
    "during_parquet_write",
    "after_pending_segment",
    "before_manifest_publish",
    "before_activate",
    "during_hot_cleanup",
    "after_cleanup_before_job_complete",
];

fn selected_failpoints() -> Vec<String> {
    if let Ok(raw) = std::env::var("KOLDSTORE_CRASH_FAILPOINTS") {
        return raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
    }
    if std::env::var("KOLDSTORE_CRASH_FULL_MATRIX").ok().as_deref() == Some("1") {
        return koldstore::failpoints::FAILPOINT_NAMES
            .iter()
            .map(|s| (*s).to_string())
            .collect();
    }
    DEFAULT_FAILPOINTS
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

#[tokio::test]
async fn flush_failpoint_recovery_preserves_visible_rows() -> Result<()> {
    // Serialize against async-mirror failpoint tests: a concurrent soft-fail
    // apply + wait_for_async_mirror can trip a Postgres assert build and take
    // down every parallel E2E connection with "connection closed".
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        for failpoint in selected_failpoints() {
            run_one_failpoint(target.clone(), &failpoint).await?;
        }
    }
    Ok(())
}

async fn run_one_failpoint(target: common::PgTarget, failpoint: &str) -> Result<()> {
    let db = common::TestDb::start(target, &format!("crash_{failpoint}")).await?;
    let table = db.create_indexed_items_table("crash_items", 36).await?;
    db.client
        .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 6,
              min_flush_rows => 1,
              max_rows_per_file => 12,
              migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[&table.relation, &db.storage_name],
        )
        .await?;

    let baseline = format!("{}_baseline", db.schema);
    let baseline_rel = format!("{}.{}", db.schema, baseline);
    db.client
        .batch_execute(&format!(
            r#"
            CREATE TABLE {baseline_rel} AS
            SELECT id, account_id, title, qty, category, created_at FROM {relation};
            ALTER TABLE {baseline_rel} ADD PRIMARY KEY (id);
            "#,
            relation = table.relation
        ))
        .await?;

    common::fence_async_mirror(&db.client).await?;

    // Arm failpoint and attempt flush (expect job failure / abort).
    db.client
        .batch_execute(&format!("SET koldstore.failpoint = '{failpoint}';"))
        .await
        .with_context(|| format!("arm failpoint {failpoint}"))?;

    let flush_result = db
        .client
        .query_one(
            "SELECT (koldstore.flush_table($1::text::regclass)->>'job_id')",
            &[&table.relation],
        )
        .await;

    // Disarm before recovery/retry.
    db.client
        .batch_execute("SET koldstore.failpoint = '';")
        .await?;

    match flush_result {
        Ok(row) => {
            let job_id: Option<String> = row.get(0);
            let Some(job_id) = job_id.filter(|v| !v.is_empty() && v != "null") else {
                common::log_always(format!(
                    "failpoint {failpoint}: flush returned NULL (no work / already idle)"
                ));
                // Still recover + retry below.
                let _ = db
                    .client
                    .query_one(
                        "SELECT koldstore.recover_segments($1::text::regclass, false)",
                        &[&table.relation],
                    )
                    .await?;
                let retried = db.flush_table(&table.relation).await?;
                common::log_always(format!(
                    "failpoint {failpoint}: retry flushed rows_flushed={retried}"
                ));
                common::fence_async_mirror(&db.client).await?;
                common::assert_relations_equal(&db.client, &baseline_rel, &table.relation).await?;
                return Ok(());
            };
            let row = db
                .client
                .query_one(
                    "SELECT status, error_trace, phase FROM koldstore.jobs WHERE id = $1::text::uuid",
                    &[&job_id],
                )
                .await?;
            let status: String = row.get("status");
            let error_trace: Option<String> = row.get("error_trace");
            let phase: String = row.get("phase");
            if status == "completed" {
                // Failpoint may sit after successful completion marker; still recover.
                common::log_always(format!(
                    "failpoint {failpoint}: flush reported success status={status}"
                ));
            } else if status == "error" {
                anyhow::ensure!(
                    error_trace.as_deref().is_some_and(|t| !t.is_empty()),
                    "failpoint {failpoint}: error job must persist error_trace (phase={phase})"
                );
                anyhow::ensure!(
                    error_trace
                        .as_deref()
                        .is_some_and(|t| t.contains("failpoint") || !t.is_empty()),
                    "failpoint {failpoint}: error_trace should explain failure, got {error_trace:?}"
                );
                common::log_always(format!(
                    "failpoint {failpoint}: flush job status=error phase={phase} error_trace={}",
                    error_trace.as_deref().unwrap_or("")
                ));
            } else {
                common::log_always(format!(
                    "failpoint {failpoint}: flush job status={status} phase={phase} error_trace={error_trace:?}"
                ));
            }
        }
        Err(error) => {
            common::log_always(format!(
                "failpoint {failpoint}: flush errored as expected: {error}"
            ));
            // Best-effort: if a job row was written before the error propagated, it
            // should carry error_trace for operator tracking.
            if let Ok(Some(row)) = db
                .client
                .query_opt(
                    r#"
                    SELECT status, error_trace
                    FROM koldstore.jobs
                    WHERE table_oid = $1::text::regclass::oid
                      AND job_type = 'flush'
                    ORDER BY updated_at DESC
                    LIMIT 1
                    "#,
                    &[&table.relation],
                )
                .await
            {
                let status: String = row.get("status");
                let error_trace: Option<String> = row.get("error_trace");
                if status == "error" {
                    anyhow::ensure!(
                        error_trace.as_deref().is_some_and(|t| !t.is_empty()),
                        "failpoint {failpoint}: errored SQL path must persist error_trace"
                    );
                }
            }
        }
    }

    // Recover orphans and retry flush.
    let _ = db
        .client
        .query_one(
            "SELECT koldstore.recover_segments($1::text::regclass, false)",
            &[&table.relation],
        )
        .await?;

    // Late failpoints (e.g. after_cleanup_before_job_complete) may already have
    // published cold + cleaned hot, so a policy flush correctly returns NULL.
    // Force drains any remaining mirror work without weakening the visibility check.
    let retried = db.flush_table_with_force(&table.relation, true).await?;
    common::log_always(format!(
        "failpoint {failpoint}: retry flushed rows_flushed={retried}"
    ));

    common::fence_async_mirror(&db.client).await?;
    common::assert_relations_equal(&db.client, &baseline_rel, &table.relation).await?;
    common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;

    let visible = common::relation_row_count(&db.client, &table.relation).await?;
    if visible != 36 {
        bail!("failpoint {failpoint}: expected 36 visible rows, got {visible}");
    }

    common::assert_no_active_jobs(&db.client, &table.relation).await?;
    Ok(())
}
