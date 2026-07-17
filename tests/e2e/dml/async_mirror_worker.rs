//! Deep async mirror database-worker lifecycle coverage.
use crate::common;

use anyhow::Result;
use std::time::{Duration, Instant};

#[tokio::test]
async fn async_worker_restarts_after_kill_and_applies_without_duplicates() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async worker lifecycle assertions in strict mode");
        return Ok(());
    }
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_mirror_worker").await?;
        clear_async_failpoint(&db.client).await?;
        cleanup_leftover_async_tables(&db.client).await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");

        ensure_publication(&db.client).await?;
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_async(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'body-' || id FROM generate_series(1, 100) id"
                ),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 100).await?;
        assert_eq!(common::mirror_op_count(&db.client, &mirror, 1).await?, 100);

        assert!(common::terminate_async_worker(&db.client).await?);

        // Launcher re-registers the applier after a crash; fall back to ensure.
        if wait_until_worker_running(&db.client).await.is_err() {
            common::wait_for_async_worker(&db.client).await?;
        }
        common::log_always("worker available again after kill");

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} SELECT id, 'body-' || id FROM generate_series(101, 150) id"
                ),
                &[],
            )
            .await?;
        let _ = common::wait_for_async_mirror(&db.client).await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 150).await?;
        assert_eq!(
            common::mirror_op_count(&db.client, &mirror, 1).await?,
            150,
            "catch-up after kill must not duplicate mirror rows"
        );
        assert_eq!(common::wait_for_async_mirror(&db.client).await?, 0);

        cleanup_async_table(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn async_worker_recovers_from_apply_failpoint_without_duplicates() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async worker failpoint assertions in strict mode");
        return Ok(());
    }
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_mirror_failpoint").await?;
        clear_async_failpoint(&db.client).await?;
        cleanup_leftover_async_tables(&db.client).await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");
        let dbname: String = db
            .client
            .query_one("SELECT current_database()::text", &[])
            .await?
            .get(0);

        ensure_publication(&db.client).await?;
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_async(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        let result = async {
            // Background workers load database defaults at connect time.
            db.client
                .batch_execute(&format!(
                    "ALTER DATABASE \"{dbname}\" SET koldstore.failpoint = 'error:async_mirror_apply'"
                ))
                .await?;
            let _ = common::terminate_async_worker(&db.client).await?;
            // Launcher re-registers; fall back to ensure if the poll window is tight.
            if wait_until_worker_running(&db.client).await.is_err() {
                common::wait_for_async_worker(&db.client).await?;
            }

            db.client
                .execute(
                    &format!("INSERT INTO {relation} (id, body) VALUES (1, 'armed')"),
                    &[],
                )
                .await?;
            // Applier ERROR-exits on the failpoint; the launcher may restart it
            // immediately, so do not require a stable stopped window.
            tokio::time::sleep(Duration::from_millis(750)).await;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        clear_async_failpoint(&db.client).await?;
        result?;

        // New backends must load the reset failpoint default.
        let _ = common::terminate_async_worker(&db.client).await?;
        if wait_until_worker_running(&db.client).await.is_err() {
            common::wait_for_async_worker(&db.client).await?;
        }
        // Catch up any WAL left from the crashed apply attempt, then insert the recovery row.
        let _ = common::wait_for_async_mirror(&db.client).await?;
        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (2, 'recovered')"),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 2).await?;
        assert_eq!(
            common::mirror_op_count(&db.client, &mirror, 1).await?,
            2,
            "failpoint recovery must apply each PK once"
        );
        assert_eq!(common::wait_for_async_mirror(&db.client).await?, 0);

        cleanup_async_table(&db.client, &relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn async_worker_respects_guc_and_cleanup_lifecycle() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async worker GUC/cleanup assertions in strict mode");
        return Ok(());
    }
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_mirror_guc").await?;
        clear_async_failpoint(&db.client).await?;
        cleanup_leftover_async_tables(&db.client).await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);

        ensure_publication(&db.client).await?;
        db.client
            .batch_execute("SET koldstore.internal_async_mirror_worker = off")
            .await?;
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        let manage_err = db
            .client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  mirror_capture_mode => 'async'
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await;
        assert!(
            manage_err.is_err(),
            "manage_table async must fail when the worker GUC is off"
        );
        db.client
            .batch_execute("RESET koldstore.internal_async_mirror_worker")
            .await?;

        manage_async(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;
        cleanup_async_table(&db.client, &relation).await?;
        force_stop_async_worker(&db.client).await?;

        let reenabled = format!("{}_re", db.schema);
        let reenabled_relation = db.relation(&reenabled);
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {reenabled_relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_async(&db.client, &reenabled_relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;
        cleanup_async_table(&db.client, &reenabled_relation).await?;
    }
    Ok(())
}

#[tokio::test]
async fn async_worker_survives_truncate_noise_in_slot() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async truncate resilience in strict mode");
        return Ok(());
    }
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_mirror_truncate").await?;
        clear_async_failpoint(&db.client).await?;
        cleanup_leftover_async_tables(&db.client).await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");
        let noise = format!("{}_noise", db.schema);
        let noise_relation = db.relation(&noise);

        ensure_publication(&db.client).await?;
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL);\
                 CREATE TABLE {noise_relation} (id bigint PRIMARY KEY, body text NOT NULL)"
            ))
            .await?;
        manage_async(&db.client, &relation, &db.storage_name).await?;
        common::wait_for_async_worker(&db.client).await?;

        // Publish a second relation, truncate it, then unpublish — leaves truncate
        // messages that older appliers treated as fatal. Keep each step in its own
        // transaction so the truncate is unambiguously published while the table
        // is still a publication member (ADD+DROP in one xact is flaky under load).
        db.client
            .batch_execute(&format!(
                "ALTER PUBLICATION koldstore_async_mirror ADD TABLE {noise_relation} (id)"
            ))
            .await?;
        db.client
            .batch_execute(&format!(
                "INSERT INTO {noise_relation} VALUES (1, 'n');\
                 TRUNCATE {noise_relation}"
            ))
            .await?;
        db.client
            .batch_execute(&format!(
                "ALTER PUBLICATION koldstore_async_mirror DROP TABLE {noise_relation}"
            ))
            .await?;

        // Drain truncate noise before the managed insert; surface apply errors
        // immediately instead of waiting for a background-worker timeout.
        let _ = common::wait_for_async_mirror(&db.client).await?;
        if !common::async_worker_running(&db.client).await? {
            common::wait_for_async_worker(&db.client).await?;
        }

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'ok')"),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 1).await?;
        assert!(
            common::async_worker_running(&db.client).await?,
            "worker must remain running after truncate noise"
        );
        assert_eq!(common::wait_for_async_mirror(&db.client).await?, 0);

        cleanup_async_table(&db.client, &relation).await?;
    }
    Ok(())
}

async fn clear_async_failpoint(client: &tokio_postgres::Client) -> Result<()> {
    let dbname: String = client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);
    client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint; RESET koldstore.failpoint"
        ))
        .await?;
    Ok(())
}

async fn cleanup_leftover_async_tables(client: &tokio_postgres::Client) -> Result<()> {
    let leftovers = client
        .query(
            "SELECT table_oid::regclass::text FROM koldstore.schemas \
             WHERE active AND COALESCE(options->>'mirror_capture_mode', 'strict') = 'async'",
            &[],
        )
        .await?;
    for row in leftovers {
        let leftover: String = row.get(0);
        let _ = client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&leftover],
            )
            .await;
    }
    let _ = client
        .query_one("SELECT koldstore.disable_async_mirror()", &[])
        .await;
    Ok(())
}

/// Terminates the applier until it stays down (slot must already be gone).
async fn force_stop_async_worker(client: &tokio_postgres::Client) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let _ = common::terminate_async_worker(client).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        if !common::async_worker_running(client).await? {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "async worker did not stay stopped within 10s"
        );
    }
}

/// Waits for postmaster auto-restart without calling ensure (no DML kick).
async fn wait_until_worker_running(client: &tokio_postgres::Client) -> Result<Duration> {
    let started = Instant::now();
    let deadline = Duration::from_secs(15);
    loop {
        if common::async_worker_running(client).await? {
            return Ok(started.elapsed());
        }
        anyhow::ensure!(
            started.elapsed() <= deadline,
            "async worker did not auto-restart within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn ensure_publication(client: &tokio_postgres::Client) -> Result<()> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'koldstore_async_mirror')",
            &[],
        )
        .await?
        .get(0);
    if !exists {
        client
            .batch_execute("CREATE PUBLICATION koldstore_async_mirror")
            .await?;
    }
    Ok(())
}

async fn manage_async(
    client: &tokio_postgres::Client,
    relation: &str,
    storage: &str,
) -> Result<()> {
    client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 1000,
              mirror_capture_mode => 'async'
            )
            "#,
            &[&relation, &storage],
        )
        .await?;
    Ok(())
}

async fn cleanup_async_table(client: &tokio_postgres::Client, relation: &str) -> Result<()> {
    let _ = client
        .query_one(
            "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
            &[&relation],
        )
        .await;
    cleanup_leftover_async_tables(client).await?;
    // Dropping the slot lets the applier exit; launcher will not re-register.
    let _ = client
        .query_one("SELECT koldstore.disable_async_mirror()", &[])
        .await;
    Ok(())
}
