//! Crash/failpoint recovery: arm flush failpoints, recover, retry, validate rows.
#[path = "../common/mod.rs"]
mod common;

use anyhow::{bail, Context, Result};

/// Default failpoints exercised in CI smoke; full matrix via env.
const DEFAULT_FAILPOINTS: &[&str] = &[
    "after_select_rows",
    "before_manifest_publish",
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
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        for failpoint in selected_failpoints() {
            run_one_failpoint(target.clone(), &failpoint).await?;
        }
    }
    Ok(())
}

async fn run_one_failpoint(target: common::PgTarget, failpoint: &str) -> Result<()> {
    let mode = common::selected_mirror_capture_mode()?.as_str();
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
              mirror_capture_mode => $3
            )
            "#,
            &[&table.relation, &db.storage_name, &mode],
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

    // Arm failpoint and attempt flush (expect job failure / abort).
    db.client
        .batch_execute(&format!("SET koldstore.failpoint = '{failpoint}';"))
        .await
        .with_context(|| format!("arm failpoint {failpoint}"))?;

    let flush_result = db
        .client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass)::text",
            &[&table.relation],
        )
        .await;

    // Disarm before recovery/retry.
    db.client
        .batch_execute("SET koldstore.failpoint = '';")
        .await?;

    match flush_result {
        Ok(row) => {
            let job_id: String = row.get(0);
            let status: String = db
                .client
                .query_one(
                    "SELECT status FROM koldstore.jobs WHERE id = $1::text::uuid",
                    &[&job_id],
                )
                .await?
                .get(0);
            if status == "completed" {
                // Failpoint may sit after successful completion marker; still recover.
                common::log_always(format!(
                    "failpoint {failpoint}: flush reported success status={status}"
                ));
            } else {
                common::log_always(format!(
                    "failpoint {failpoint}: flush job status={status} (expected non-success)"
                ));
            }
        }
        Err(error) => {
            common::log_always(format!(
                "failpoint {failpoint}: flush errored as expected: {error}"
            ));
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

    let retried = db.flush_table(&table.relation).await?;
    common::log_always(format!(
        "failpoint {failpoint}: retry flushed rows_flushed={retried}"
    ));

    common::assert_relations_equal(&db.client, &baseline_rel, &table.relation).await?;
    common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;

    let visible = common::relation_row_count(&db.client, &table.relation).await?;
    if visible != 36 {
        bail!("failpoint {failpoint}: expected 36 visible rows, got {visible}");
    }

    common::assert_no_active_jobs(&db.client, &table.relation).await?;
    Ok(())
}
