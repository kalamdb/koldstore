//! Autonomous WAL-to-flush liveness and generation-race coverage.

use std::time::Duration;

use anyhow::{Context, Result};

use crate::{common, flush::harness};

const CONVERGENCE_DEADLINE: Duration = Duration::from_secs(60);

async fn database_name(client: &tokio_postgres::Client) -> Result<String> {
    Ok(client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0))
}

async fn completed_jobs(client: &tokio_postgres::Client, relation: &str) -> Result<i64> {
    Ok(client
        .query_one(
            r#"
            SELECT count(*)::bigint
            FROM koldstore.jobs j
            WHERE j.table_oid = $1::text::regclass::oid
              AND j.job_type = 'flush'
              AND j.status = 'completed'
              AND j.created_at >= (
                  SELECT created_at FROM koldstore.schemas
                  WHERE table_oid = $1::text::regclass::oid AND active
              )
            "#,
            &[&relation],
        )
        .await?
        .get(0))
}

async fn manage_auto(
    db: &common::TestDb,
    relation: &str,
    hot_row_limit: i64,
    max_rows_per_file: i64,
) -> Result<()> {
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
                table_name => $1::text::regclass,
                storage => $2,
                hot_row_limit => $3,
                min_flush_rows => 1,
                max_rows_per_file => $4,
                migration_order_by => 'id',
                auto_flush => true
            )
            "#,
            &[
                &relation,
                &db.storage_name,
                &hot_row_limit,
                &max_rows_per_file,
            ],
        )
        .await?;
    Ok(())
}

/// DML committed while an automatic flush is active must be absorbed by that
/// job or a follow-up job without a fence, ensure, tick, retry, or restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_flush_converges_writes_arriving_during_active_flush() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "auto_flush_followup").await?;
    let table_name = format!("{}_events", db.schema);
    let relation = db.relation(&table_name);
    let dbname = database_name(&db.client).await?;

    db.client
        .batch_execute(&format!(
            r#"
            ALTER DATABASE "{dbname}" SET koldstore.flush_execution = 'queue';
            ALTER DATABASE "{dbname}" SET koldstore.flush_check_interval_seconds = 1;
            ALTER DATABASE "{dbname}" SET koldstore.max_parallel_flush_jobs = 1;
            ALTER DATABASE "{dbname}" SET koldstore.min_max_rows_per_file = 1;
            ALTER DATABASE "{dbname}" SET koldstore.failpoint = 'wait:after_cleanup_before_job_complete';
            SET koldstore.flush_execution = 'queue';
            SET koldstore.min_max_rows_per_file = 1;
            CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL);
            "#
        ))
        .await?;
    manage_auto(&db, &relation, 8, 16).await?;
    let original_wal_pid =
        common::wait_for_wal_applier_passively(&db.client, Duration::from_secs(30)).await?;

    let coordinator = harness::connect_peer(&db).await?;
    harness::barrier_lock(&coordinator).await?;
    db.client
        .batch_execute(&format!(
            "INSERT INTO {relation} SELECT id, 'wave-1-' || id FROM generate_series(1, 64) id"
        ))
        .await?;
    harness::wait_until_barrier_waiter(&coordinator, || false).await?;

    db.client
        .batch_execute(&format!(
            r#"
            INSERT INTO {relation}
            SELECT id, 'wave-2-' || id FROM generate_series(65, 128) id;
            UPDATE {relation} SET body = 'updated-after-first-selection' WHERE id = 1;
            DELETE FROM {relation} WHERE id = 2;
            INSERT INTO {relation} VALUES (2, 'reinserted-after-delete');
            "#
        ))
        .await?;
    let target_lsn = common::current_wal_lsn(&db.client).await?;
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint"
        ))
        .await?;
    harness::barrier_unlock(&coordinator).await?;

    let state = common::wait_for_passive_convergence(
        &db.client,
        &relation,
        &target_lsn,
        1,
        8,
        CONVERGENCE_DEADLINE,
    )
    .await?;
    anyhow::ensure!(state.wal_pid == Some(original_wal_pid));
    anyhow::ensure!(common::relation_row_count(&db.client, &relation).await? == 128);
    let bodies = db
        .client
        .query(
            &format!("SELECT id, body FROM {relation} WHERE id IN (1, 2) ORDER BY id"),
            &[],
        )
        .await?;
    anyhow::ensure!(bodies[0].get::<_, String>(1) == "updated-after-first-selection");
    anyhow::ensure!(bodies[1].get::<_, String>(1) == "reinserted-after-delete");
    common::assert_pk_unique(&db.client, &relation, &["id"]).await?;
    Ok(())
}

/// A queue generation published while the prior executor is blocked must not
/// be acknowledged by that older executor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_generation_published_during_blocked_executor_is_not_lost() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "flush_generation_race").await?;
    let table_a_name = format!("{}_table_a", db.schema);
    let table_b_name = format!("{}_table_b", db.schema);
    let table_a = db.create_indexed_items_table(&table_a_name, 64).await?;
    let table_b = db.create_indexed_items_table(&table_b_name, 64).await?;
    let dbname = database_name(&db.client).await?;
    db.client
        .batch_execute(&format!(
            r#"
            ALTER DATABASE "{dbname}" SET koldstore.flush_execution = 'queue';
            ALTER DATABASE "{dbname}" SET koldstore.max_parallel_flush_jobs = 1;
            ALTER DATABASE "{dbname}" SET koldstore.min_max_rows_per_file = 1;
            ALTER DATABASE "{dbname}" SET koldstore.failpoint = 'wait:after_select_rows';
            SET koldstore.flush_execution = 'queue';
            SET koldstore.min_max_rows_per_file = 1;
            "#
        ))
        .await?;
    for relation in [&table_a.relation, &table_b.relation] {
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                    table_name => $1::text::regclass,
                    storage => $2,
                    hot_row_limit => 4,
                    min_flush_rows => 1,
                    max_rows_per_file => 8,
                    migration_order_by => 'id',
                    auto_flush => false
                )
                "#,
                &[relation, &db.storage_name],
            )
            .await?;
    }
    common::fence_async_mirror(&db.client).await?;

    let coordinator = harness::connect_peer(&db).await?;
    harness::barrier_lock(&coordinator).await?;
    let job_a: String = db
        .client
        .query_one(
            "SELECT koldstore.enqueue_flush_job(table_name => $1::text::regclass, force => true)::text",
            &[&table_a.relation],
        )
        .await?
        .get(0);
    harness::wait_until_barrier_waiter(&coordinator, || false).await?;
    let job_b: String = db
        .client
        .query_one(
            "SELECT koldstore.enqueue_flush_job(table_name => $1::text::regclass, force => true)::text",
            &[&table_b.relation],
        )
        .await?
        .get(0);
    db.client
        .batch_execute(&format!(
            "ALTER DATABASE \"{dbname}\" RESET koldstore.failpoint"
        ))
        .await?;
    harness::barrier_unlock(&coordinator).await?;
    common::wait_for_flush_job_terminal(&db.client, &job_a).await?;
    common::wait_for_flush_job_terminal(&db.client, &job_b).await?;
    common::assert_no_active_jobs(&db.client, &table_a.relation).await?;
    common::assert_no_active_jobs(&db.client, &table_b.relation).await?;
    Ok(())
}

/// Flush cleanup WAL must not be decoded as fresh user DML and create an
/// endless chain of automatic jobs.
#[tokio::test]
async fn automatic_flush_does_not_trigger_itself_while_idle() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "auto_flush_idle_loop").await?;
    let table_name = format!("{}_events", db.schema);
    let relation = db.relation(&table_name);
    let dbname = database_name(&db.client).await?;
    db.client
        .batch_execute(&format!(
            r#"
            ALTER DATABASE "{dbname}" SET koldstore.flush_execution = 'queue';
            ALTER DATABASE "{dbname}" SET koldstore.flush_check_interval_seconds = 1;
            ALTER DATABASE "{dbname}" SET koldstore.min_max_rows_per_file = 1;
            SET koldstore.flush_execution = 'queue';
            SET koldstore.min_max_rows_per_file = 1;
            CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL);
            "#
        ))
        .await?;
    manage_auto(&db, &relation, 5, 5).await?;
    let wal_pid =
        common::wait_for_wal_applier_passively(&db.client, Duration::from_secs(30)).await?;
    db.client
        .batch_execute(&format!(
            "INSERT INTO {relation} SELECT id, 'body-' || id FROM generate_series(1, 20) id"
        ))
        .await?;
    let target_lsn = common::current_wal_lsn(&db.client).await?;
    let settled = common::wait_for_passive_convergence(
        &db.client,
        &relation,
        &target_lsn,
        1,
        5,
        CONVERGENCE_DEADLINE,
    )
    .await?;
    let completed = completed_jobs(&db.client, &relation).await?;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let still_settled = common::wait_for_passive_convergence(
        &db.client,
        &relation,
        &target_lsn,
        completed,
        5,
        Duration::from_secs(5),
    )
    .await?;
    anyhow::ensure!(completed_jobs(&db.client, &relation).await? == completed);
    anyhow::ensure!(settled.wal_pid == Some(wal_pid));
    anyhow::ensure!(still_settled.wal_pid == Some(wal_pid));
    anyhow::ensure!(still_settled.wal_generation == still_settled.wal_processed_generation);
    Ok(())
}

/// A source transaction larger than one decoder fetch must become visible in
/// one mirror transaction, honor savepoint rollback, and then auto-flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_transaction_with_savepoint_applies_atomically_then_auto_flushes() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "large_txn_auto_flush").await?;
    let table_name = format!("{}_events", db.schema);
    let relation = db.relation(&table_name);
    let mirror = common::change_log_mirror_relation(&relation);
    let dbname = database_name(&db.client).await?;
    db.client
        .batch_execute(&format!(
            r#"
            ALTER DATABASE "{dbname}" SET koldstore.flush_execution = 'queue';
            ALTER DATABASE "{dbname}" SET koldstore.flush_check_interval_seconds = 1;
            ALTER DATABASE "{dbname}" SET koldstore.min_max_rows_per_file = 1;
            SET koldstore.flush_execution = 'queue';
            SET koldstore.min_max_rows_per_file = 1;
            CREATE TABLE {relation} (id bigint PRIMARY KEY, body text NOT NULL);
            "#
        ))
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
                table_name => $1::text::regclass,
                storage => $2,
                hot_row_limit => 100,
                min_flush_rows => 1,
                max_rows_per_file => 1000,
                migration_order_by => 'id',
                auto_flush => false
            )
            "#,
            &[&relation, &db.storage_name],
        )
        .await?;
    common::wait_for_wal_applier_passively(&db.client, Duration::from_secs(30)).await?;

    db.client
        .batch_execute(&format!(
            r#"
            BEGIN;
            INSERT INTO {relation}
            SELECT id, 'insert-' || id FROM generate_series(1, 12000) id;
            SAVEPOINT rejected_work;
            UPDATE {relation} SET body = 'must-be-rolled-back' WHERE id BETWEEN 1 AND 2000;
            ROLLBACK TO SAVEPOINT rejected_work;
            UPDATE {relation} SET body = 'committed-update' WHERE id BETWEEN 2001 AND 4000;
            DELETE FROM {relation} WHERE id % 10 = 0;
            COMMIT;
            "#
        ))
        .await?;
    let target_lsn = common::current_wal_lsn(&db.client).await?;

    let observer = common::connect(&db.target).await?;
    let started = std::time::Instant::now();
    loop {
        let count: i64 = observer
            .query_one(&format!("SELECT count(*)::bigint FROM {mirror}"), &[])
            .await?
            .get(0);
        anyhow::ensure!(
            count == 0 || count == 12_000,
            "another backend observed a partial source transaction: {count}"
        );
        if count == 12_000 {
            break;
        }
        anyhow::ensure!(
            started.elapsed() <= Duration::from_secs(30),
            "large source transaction was not applied within 30s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let operations = db
        .client
        .query_one(
            &format!(
                r#"
                SELECT
                    count(*) FILTER (WHERE op = 1)::bigint,
                    count(*) FILTER (WHERE op = 2)::bigint,
                    count(*) FILTER (WHERE op = 3)::bigint
                FROM {mirror}
                "#
            ),
            &[],
        )
        .await?;
    anyhow::ensure!(operations.get::<_, i64>(0) == 9_000);
    anyhow::ensure!(operations.get::<_, i64>(1) == 1_800);
    anyhow::ensure!(operations.get::<_, i64>(2) == 1_200);

    db.client
        .execute(
            "SELECT koldstore.set_table_auto_flush($1::text::regclass, true)",
            &[&relation],
        )
        .await?;
    common::wait_for_passive_convergence(
        &db.client,
        &relation,
        &target_lsn,
        1,
        100,
        Duration::from_secs(90),
    )
    .await?;
    anyhow::ensure!(common::relation_row_count(&db.client, &relation).await? == 10_800);
    let rolled_back: i64 = db
        .client
        .query_one(
            &format!("SELECT count(*)::bigint FROM {relation} WHERE body = 'must-be-rolled-back'"),
            &[],
        )
        .await?
        .get(0);
    anyhow::ensure!(rolled_back == 0);
    common::assert_pk_unique(&db.client, &relation, &["id"]).await?;
    Ok(())
}
