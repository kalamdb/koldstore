use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::common;

const VISIBILITY_BOUND: Duration = Duration::from_secs(1);
const IDLE_PROBE: Duration = Duration::from_secs(2);

async fn wal_applier_pid(client: &tokio_postgres::Client) -> Result<i32> {
    client
        .query_one(
            "SELECT (koldstore.async_mirror_status()->'wal_applier'->>'pid')::integer",
            &[],
        )
        .await?
        .get::<_, Option<i32>>(0)
        .context("persistent WAL applier has no published PID")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_wal_applier_keeps_pid_while_idle_and_wakes_within_slo() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "persistent_wal_idle").await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);
        let mirror = common::change_log_mirror_relation(&relation);

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;
        common::fence_async_mirror(&db.client).await?;

        let before_pid = wal_applier_pid(&db.client).await?;
        tokio::time::sleep(IDLE_PROBE).await;
        let after_idle_pid = wal_applier_pid(&db.client).await?;
        assert_eq!(
            after_idle_pid, before_pid,
            "WAL applier must remain registered across idle periods instead of exiting"
        );

        let idle_state = db
            .client
            .query_one(
                "SELECT xact_start IS NULL \
                 FROM pg_catalog.pg_stat_activity WHERE pid = $1",
                &[&before_pid],
            )
            .await?;
        assert!(
            idle_state.get::<_, bool>(0),
            "idle WAL applier must not retain an open transaction or MVCC snapshot"
        );

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'wake')"),
                &[],
            )
            .await?;
        let committed_at = Instant::now();
        loop {
            let found: bool = db
                .client
                .query_one(
                    &format!("SELECT EXISTS (SELECT 1 FROM {mirror} WHERE id = 1 AND op = 1)"),
                    &[],
                )
                .await?
                .get(0);
            if found {
                break;
            }
            anyhow::ensure!(
                committed_at.elapsed() < VISIBILITY_BOUND,
                "persistent WAL applier did not publish the committed insert within {VISIBILITY_BOUND:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let after_apply_pid = wal_applier_pid(&db.client).await?;
        assert_eq!(
            after_apply_pid, before_pid,
            "normal commit application must wake the resident process, not replace it"
        );

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&relation],
            )
            .await?;
        let _ = db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_replaces_idle_wal_applier_without_new_dml() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "persistent_wal_restart").await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);

        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  auto_flush => false
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;
        common::fence_async_mirror(&db.client).await?;

        let original_pid = wal_applier_pid(&db.client).await?;
        anyhow::ensure!(
            common::terminate_async_worker(&db.client).await?,
            "expected to terminate the resident WAL applier"
        );
        common::wait_for_async_worker_auto_restart(&db.client).await?;
        let replacement_pid = wal_applier_pid(&db.client).await?;
        assert_ne!(
            replacement_pid, original_pid,
            "supervisor must replace a lost required WAL service even while caught up"
        );

        db.client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&relation],
            )
            .await?;
        let _ = db
            .client
            .query_one("SELECT koldstore.disable_async_mirror()", &[])
            .await?;
    }
    Ok(())
}
