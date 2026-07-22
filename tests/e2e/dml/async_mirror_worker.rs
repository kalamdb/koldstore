//! Deep async mirror database-worker lifecycle coverage.
use crate::common;

use anyhow::Result;
use std::time::{Duration, Instant};

#[tokio::test]
async fn async_apply_drains_above_retained_wal_health_threshold() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async retained-WAL drain assertion in strict mode");
        return Ok(());
    }
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_retained_wal_drain").await?;
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
            .batch_execute(
                "SET koldstore.internal_async_mirror_worker = off; \
                 SET koldstore.async_mirror_max_retained_bytes = 1",
            )
            .await?;
        force_stop_async_worker(&db.client).await?;
        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} \
                     SELECT id, 'body-' || id FROM generate_series(1, 100) id"
                ),
                &[],
            )
            .await?;

        let applied = common::wait_for_async_mirror(&db.client).await?;
        assert_eq!(
            applied, 100,
            "the fence must drain retained WAL above the health threshold"
        );
        assert_eq!(common::mirror_op_count(&db.client, &mirror, 1).await?, 100);

        db.client
            .batch_execute(
                "RESET koldstore.async_mirror_max_retained_bytes; \
                 RESET koldstore.internal_async_mirror_worker",
            )
            .await?;
        cleanup_async_table(&db.client, &relation).await?;
    }
    Ok(())
}

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

        // Dynamic appliers are BGW_NEVER_RESTART and E2E does not preload the
        // launcher — bounce via ensure instead of waiting for auto-restart.
        force_stop_async_worker(&db.client).await?;
        common::wait_for_async_worker(&db.client).await?;
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
            // Fully stop before ensure so the restarted applier loads the new GUC.
            force_stop_async_worker(&db.client).await?;
            common::wait_for_async_worker(&db.client).await?;

            db.client
                .execute(
                    &format!("INSERT INTO {relation} (id, body) VALUES (1, 'armed')"),
                    &[],
                )
                .await?;
            // Applier soft-fails on the failpoint and backs off; it must remain
            // running so catch-up resumes after the failpoint is cleared.
            tokio::time::sleep(Duration::from_millis(500)).await;
            let soft_fail_deadline = Instant::now() + Duration::from_secs(3);
            loop {
                if common::async_worker_running(&db.client).await? {
                    break;
                }
                anyhow::ensure!(
                    Instant::now() < soft_fail_deadline,
                    "soft-fail must keep the async worker alive"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        clear_async_failpoint(&db.client).await?;
        result?;

        // Worker keeps the old failpoint until reconnect; bounce after reset.
        // NEVER_RESTART appliers do not auto-respawn — ensure explicitly.
        force_stop_async_worker(&db.client).await?;
        common::wait_for_async_worker(&db.client).await?;
        // Catch up any WAL left from the soft-failed apply attempt, then insert the recovery row.
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

/// Mid-tick abort after mirror SPI writes must roll back `applied_lsn` and
/// mirror rows together (one PostgreSQL transaction per apply tick).
#[tokio::test]
async fn async_apply_mid_tick_abort_rolls_back_applied_lsn() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping mid-tick applied_lsn abort assertions in strict mode");
        return Ok(());
    }

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_mid_tick_abort").await?;
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

        // Arm the failpoint on the session and keep the bgworker down so only
        // wait_for_async_mirror consumes WAL. A racing worker without the
        // failpoint would apply successfully and make the fence return Ok.
        db.client
            .batch_execute(&format!(
                "ALTER DATABASE \"{dbname}\" SET koldstore.failpoint = 'error:async_mirror_apply_after_batch'; \
                 SET koldstore.failpoint = 'error:async_mirror_apply_after_batch'; \
                 SET koldstore.internal_async_mirror_worker = off"
            ))
            .await?;
        force_stop_async_worker(&db.client).await?;

        let before: Option<String> = db
            .client
            .query_opt(
                "SELECT applied_lsn::text FROM koldstore.async_mirror_state \
                 WHERE database_oid = (SELECT oid FROM pg_database WHERE datname = current_database())",
                &[],
            )
            .await?
            .map(|row| row.get(0));

        db.client
            .execute(
                &format!("INSERT INTO {relation} (id, body) VALUES (1, 'partial')"),
                &[],
            )
            .await?;

        // Session fence must ERROR when it hits the after-batch failpoint.
        let err = db
            .client
            .query_one("SELECT koldstore.wait_for_async_mirror()", &[])
            .await;
        assert!(
            err.is_err(),
            "wait_for_async_mirror must ERROR on mid-tick after-batch failpoint; got {err:?}"
        );

        let after: Option<String> = db
            .client
            .query_opt(
                "SELECT applied_lsn::text FROM koldstore.async_mirror_state \
                 WHERE database_oid = (SELECT oid FROM pg_database WHERE datname = current_database())",
                &[],
            )
            .await?
            .map(|row| row.get(0));
        assert_eq!(
            before, after,
            "mid-tick abort must not durably advance applied_lsn"
        );
        assert_eq!(
            common::mirror_op_count(&db.client, &mirror, 1).await?,
            0,
            "mid-tick abort must roll back mirror SPI writes with applied_lsn"
        );

        clear_async_failpoint(&db.client).await?;
        // Re-enable the worker and catch up the rolled-back change exactly once.
        common::wait_for_async_worker(&db.client).await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 1).await?;
        assert_eq!(
            common::mirror_op_count(&db.client, &mirror, 1).await?,
            1,
            "recovery must apply the rolled-back change exactly once"
        );
        assert_eq!(common::wait_for_async_mirror(&db.client).await?, 0);

        cleanup_async_table(&db.client, &relation).await?;
    }
    Ok(())
}

/// Regression: idle ticks must not re-decode retained WAL or rewrite mirror
/// state when only non-publication WAL is generated.
///
/// Before the fix, every latch wake called `pg_replication_slot_advance` /
/// peek against a lagged `restart_lsn`, pinning a core and flooding
/// "starting logical decoding" logs while `applied_lsn` stayed put.
#[tokio::test]
async fn async_idle_non_publication_wal_advances_slot_without_reapply() -> Result<()> {
    if !common::selected_mirror_capture_mode()?.is_async() {
        common::log_always("skipping async idle WAL retention regression in strict mode");
        return Ok(());
    }
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "async_idle_wal_skip").await?;
        clear_async_failpoint(&db.client).await?;
        cleanup_leftover_async_tables(&db.client).await?;
        let table_name = format!("{}_events", db.schema);
        let relation = db.relation(&table_name);
        let mirror = format!("koldstore.{table_name}__cl");
        let noise_name = format!("{}_noise", db.schema);
        let noise = db.relation(&noise_name);

        ensure_publication(&db.client).await?;
        db.client
            .batch_execute(&format!(
                "CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL); \
                 CREATE TABLE {noise} (id bigserial PRIMARY KEY, payload text NOT NULL)"
            ))
            .await?;
        // auto_flush off so the flush scheduler cannot enqueue work during idle.
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 1000,
                  auto_flush => false,
                  mirror_capture_mode => 'async'
                )
                "#,
                &[&relation, &db.storage_name],
            )
            .await?;
        common::wait_for_async_worker(&db.client).await?;

        db.client
            .execute(
                &format!(
                    "INSERT INTO {relation} \
                     SELECT id, 'body-' || id FROM generate_series(1, 50) id"
                ),
                &[],
            )
            .await?;
        common::wait_for_mirror_op_count(&db.client, &mirror, 1, 50).await?;
        assert_eq!(common::wait_for_async_mirror(&db.client).await?, 0);

        let baseline = common::async_mirror_progress(&db.client).await?;
        anyhow::ensure!(
            baseline.applied_lsn.is_some(),
            "expected durable applied_lsn after initial catch-up"
        );
        let jobs_before: i64 = db
            .client
            .query_one("SELECT count(*)::bigint FROM koldstore.jobs", &[])
            .await?
            .get(0);

        // Stop the applier so non-publication WAL accumulates behind confirmed_flush
        // (the historical pin-CPU shape: large restart→current gap on every wake).
        force_stop_async_worker(&db.client).await?;
        db.client
            .batch_execute(&format!(
                "INSERT INTO {noise} (payload) \
                 SELECT repeat('x', 4096) FROM generate_series(1, 2000); \
                 SELECT pg_switch_wal()"
            ))
            .await?;
        let blocked = common::async_mirror_progress(&db.client).await?;
        anyhow::ensure!(
            blocked.retained_bytes > 256 * 1024,
            "expected retained WAL behind a stopped slot, got {} bytes",
            blocked.retained_bytes
        );
        common::log_always(format!(
            "accumulated {} bytes of non-publication WAL behind stopped applier",
            blocked.retained_bytes
        ));

        common::wait_for_async_worker(&db.client).await?;
        let drained = common::wait_for_confirmed_flush_past(
            &db.client,
            &baseline.confirmed_flush_lsn,
            Duration::from_secs(15),
        )
        .await?;
        common::log_always(format!(
            "idle empty-peek advanced confirmed_flush {} -> {} (retained_bytes {} -> {})",
            baseline.confirmed_flush_lsn,
            drained.confirmed_flush_lsn,
            blocked.retained_bytes,
            drained.retained_bytes
        ));

        assert_eq!(
            drained.applied_lsn, baseline.applied_lsn,
            "empty non-publication WAL must not rewrite applied_lsn"
        );
        assert_eq!(
            drained.updated_at, baseline.updated_at,
            "empty non-publication WAL must not bump async_mirror_state.updated_at"
        );
        assert_eq!(
            common::mirror_op_count(&db.client, &mirror, 1).await?,
            50,
            "idle path must not rewrite mirror rows"
        );
        assert!(
            drained.retained_bytes < blocked.retained_bytes / 2,
            "confirmed_flush advance must shrink retention substantially \
             (before={}, after={})",
            blocked.retained_bytes,
            drained.retained_bytes
        );
        assert!(
            drained.retained_bytes < 2 * 1024 * 1024,
            "retention after idle advance should be small, got {} bytes",
            drained.retained_bytes
        );

        // Once caught up, further idle wakes must stay quiet: confirmed_flush
        // should not thrash and fences must remain empty/cheap.
        let quiet = common::async_mirror_progress(&db.client).await?;
        let mut confirmed_changes = 0_u32;
        let mut last_confirmed = quiet.confirmed_flush_lsn.clone();
        let sample_started = Instant::now();
        while sample_started.elapsed() < Duration::from_secs(2) {
            let sample = common::async_mirror_progress(&db.client).await?;
            if sample.confirmed_flush_lsn != last_confirmed {
                confirmed_changes += 1;
                last_confirmed = sample.confirmed_flush_lsn;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            confirmed_changes <= 8,
            "caught-up idle worker must not thrash confirmed_flush (saw {confirmed_changes} changes)"
        );

        for _ in 0..5 {
            let fence_started = Instant::now();
            let applied = common::wait_for_async_mirror(&db.client).await?;
            assert_eq!(applied, 0, "idle fence must report no publication changes");
            assert!(
                fence_started.elapsed() < Duration::from_millis(500),
                "idle fence took {:?}; decode path still looks hot",
                fence_started.elapsed()
            );
        }

        let jobs_after: i64 = db
            .client
            .query_one("SELECT count(*)::bigint FROM koldstore.jobs", &[])
            .await?
            .get(0);
        assert_eq!(
            jobs_after, jobs_before,
            "idle non-publication WAL must not enqueue flush/migrate jobs"
        );

        cleanup_async_table(&db.client, &relation).await?;
        db.client
            .batch_execute(&format!("DROP TABLE IF EXISTS {noise}"))
            .await?;
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
            "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint; \
             ALTER DATABASE \"{dbname}\" RESET koldstore.internal_async_mirror_worker; \
             RESET koldstore.failpoint; \
             RESET koldstore.internal_async_mirror_worker"
        ))
        .await?;
    let armed: String = client
        .query_one("SHOW koldstore.failpoint", &[])
        .await?
        .get(0);
    anyhow::ensure!(
        armed.is_empty(),
        "failpoint GUC still armed after clear: {armed:?}"
    );
    // Running workers keep prior ALTER DATABASE GUCs until reconnect.
    force_stop_async_worker(client).await?;
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

/// Terminates the applier until it stays down.
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
