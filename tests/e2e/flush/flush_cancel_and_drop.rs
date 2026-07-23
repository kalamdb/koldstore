//! Cooperative cancel and DROP TABLE cleanup for flush jobs (Phase C).

use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::task::JoinHandle;

use crate::common;
use crate::flush::harness::{
    barrier_lock, barrier_unlock, connect_peer, wait_until_barrier_waiter,
};

#[tokio::test]
async fn cancel_pending_flush_job_marks_cancelled() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cancel_pending").await?;
        let table = db
            .create_indexed_items_table("cancel_pending_items", 24)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        db.client
            .batch_execute(&format!(
                "SELECT koldstore.set_table_auto_flush('{relation}'::regclass, false)",
                relation = table.relation
            ))
            .await
            .context("set_table_auto_flush(false)")?;

        let inserted = db
            .client
            .query_one(
                "SELECT koldstore.enqueue_flush_job($1::text::regclass)",
                &[&table.relation],
            )
            .await
            .context("enqueue_flush_job")?
            .get::<_, i64>(0);
        assert_eq!(inserted, 1);

        let job_id = db
            .client
            .query_one(
                r#"
                SELECT id::text
                FROM koldstore.jobs
                WHERE table_oid = $1::text::regclass::oid
                  AND job_type = 'flush'
                  AND status = 'pending'
                ORDER BY created_at DESC
                LIMIT 1
                "#,
                &[&table.relation],
            )
            .await
            .context("lookup pending job")?
            .get::<_, String>(0);

        let cancelled = db
            .client
            .query_one("SELECT koldstore.cancel_job($1::text::uuid)", &[&job_id])
            .await
            .context("cancel_job")?
            .get::<_, bool>(0);
        assert!(cancelled, "cancel_job should signal the pending job");

        let status = db
            .client
            .query_one(
                "SELECT status, cancel_requested_at IS NOT NULL AS flagged FROM koldstore.jobs WHERE id = $1::text::uuid",
                &[&job_id],
            )
            .await?;
        assert_eq!(status.get::<_, String>(0), "cancelled");
        assert!(status.get::<_, bool>(1));
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_running_flush_before_activate_marks_cancelled() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "cancel_running").await?;
        let table = db
            .create_indexed_items_table("cancel_running_items", 48)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        db.client
            .batch_execute(&format!(
                "SELECT koldstore.set_table_auto_flush('{relation}'::regclass, false)",
                relation = table.relation
            ))
            .await
            .ok();

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = table.relation.clone();
        let flush_handle: JoinHandle<Result<String>> = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await?;
            let row = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await
                .context("flush_table under cancel")?;
            flush_client
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            Ok(row.get::<_, String>(0))
        });

        wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await?;

        let cancelled = db
            .client
            .query_one(
                "SELECT koldstore.cancel_table_jobs($1::text::regclass)",
                &[&table.relation],
            )
            .await
            .context("cancel_table_jobs")?
            .get::<_, i64>(0);
        assert!(
            cancelled >= 1,
            "expected cancel_table_jobs to record a cancel request, got {cancelled}"
        );

        barrier_unlock(&coordinator).await?;
        let job_id = flush_handle.await??;

        let row = db
            .client
            .query_one(
                r#"
                SELECT status, phase,
                       COALESCE((payload->>'cancel_requested_after_publish')::boolean, false) AS after_publish
                FROM koldstore.jobs
                WHERE id = $1::text::uuid
                "#,
                &[&job_id],
            )
            .await
            .context("read cancelled job")?;
        assert_eq!(
            row.get::<_, String>("status"),
            "cancelled",
            "pre-activate cancel must stay cancelled"
        );
        assert_eq!(row.get::<_, String>("phase"), "cancelled");
        assert!(!row.get::<_, bool>("after_publish"));
    }

    Ok(())
}

#[tokio::test]
async fn drop_table_cancels_jobs_and_deletes_cold_objects() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "drop_cleanup").await?;
        let table = db
            .create_indexed_items_table("drop_cleanup_items", 36)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        db.client
            .batch_execute(&format!(
                "SELECT koldstore.set_table_auto_flush('{relation}'::regclass, false)",
                relation = table.relation
            ))
            .await
            .ok();

        let _ = db.flush_table(&table.relation).await?;

        let prefix = {
            let parts: Vec<&str> = table.relation.split('.').collect();
            format!("{}/{}", parts[0], parts[1])
        };
        let before = count_prefix_files(&db.storage_root, &prefix)?;
        assert!(
            before > 0,
            "expected cold objects before DROP, prefix={prefix}"
        );

        let table_oid = db
            .client
            .query_one("SELECT $1::text::regclass::oid::bigint", &[&table.relation])
            .await?
            .get::<_, i64>(0);

        let pending = db.insert_pending_flush_job(&table.relation).await?;
        assert_eq!(pending, 1);

        db.client
            .batch_execute(&format!("DROP TABLE {}", table.relation))
            .await
            .context("DROP TABLE")?;

        // Allow filesystem GC to settle for local storage.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let after = count_prefix_files(&db.storage_root, &prefix)?;
        assert_eq!(after, 0, "DROP must delete cold objects under {prefix}");

        let cleanup_jobs = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.jobs
                WHERE job_type = 'drop_table_cleanup'
                  AND status = 'completed'
                  AND table_oid = $1::bigint::oid
                  AND payload->>'policy' = 'delete'
                "#,
                &[&table_oid],
            )
            .await?
            .get::<_, i64>(0);
        assert!(
            cleanup_jobs >= 1,
            "expected completed drop_table_cleanup audit job"
        );

        let schemas = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.schemas
                WHERE active
                  AND table_oid = $1::bigint::oid
                "#,
                &[&table_oid],
            )
            .await?
            .get::<_, i64>(0);
        assert_eq!(schemas, 0, "managed schema must be deactivated");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_table_during_flush_waits_for_cancel_then_succeeds() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "drop_during_flush").await?;
        let table = db
            .create_indexed_items_table("drop_during_flush_items", 48)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        db.client
            .batch_execute(&format!(
                "SELECT koldstore.set_table_auto_flush('{relation}'::regclass, false)",
                relation = table.relation
            ))
            .await
            .ok();

        let coordinator = connect_peer(&db).await?;
        barrier_lock(&coordinator).await?;

        let flush_client = connect_peer(&db).await?;
        let flush_relation = table.relation.clone();
        let flush_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            flush_client
                .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
                .await?;
            let result = flush_client
                .query_one(
                    "SELECT koldstore.flush_table($1::text::regclass)::text",
                    &[&flush_relation],
                )
                .await;
            flush_client
                .batch_execute("SET koldstore.failpoint = '';")
                .await
                .ok();
            match result {
                Ok(_) => Ok(()),
                Err(error) => {
                    let detail = error.to_string();
                    if detail.contains("does not exist")
                        || detail.contains("cancel")
                        || detail.contains("managed schema")
                        || detail.contains("flush")
                    {
                        Ok(())
                    } else {
                        Err(error.into())
                    }
                }
            }
        });

        wait_until_barrier_waiter(&coordinator, || flush_handle.is_finished()).await?;

        // DROP signals cancel then waits on the table-job advisory lock. Unlock
        // the failpoint so flush can observe cancel, exit, and release the lock.
        let drop_client = connect_peer(&db).await?;
        let drop_relation = table.relation.clone();
        let drop_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            drop_client
                .batch_execute(&format!("DROP TABLE {drop_relation}"))
                .await
                .context("DROP TABLE during flush")?;
            Ok(())
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        barrier_unlock(&coordinator).await?;
        flush_handle.await??;
        drop_handle.await??;

        let active = db
            .client
            .query_one(
                r#"
                SELECT count(*)::bigint
                FROM koldstore.jobs
                WHERE job_type = 'flush'
                  AND status IN ('pending', 'running')
                "#,
                &[],
            )
            .await?
            .get::<_, i64>(0);
        if active != 0 {
            bail!("flush jobs must not stay active after DROP, got {active}");
        }

        let exists = db
            .client
            .query_one(
                "SELECT to_regclass($1::text) IS NOT NULL",
                &[&table.relation],
            )
            .await?
            .get::<_, bool>(0);
        assert!(!exists, "table must be gone after DROP");
    }

    Ok(())
}

fn count_prefix_files(root: &std::path::Path, prefix: &str) -> Result<usize> {
    let dir = root.join(prefix);
    if !dir.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in walkdir_files(&dir)? {
        count = count.saturating_add(1);
        let _ = entry;
    }
    Ok(count)
}

fn walkdir_files(dir: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            out.extend(walkdir_files(&path)?);
        } else {
            out.push(path);
        }
    }
    Ok(out)
}
