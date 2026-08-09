//! Flush-queue liveness when early candidates are temporarily unclaimable.

use std::time::Duration;

use anyhow::{Context, Result};

use crate::common;

const TABLE_JOB_LOCK_NAMESPACE: i64 = 0x4b54_4a42;
const BUSY_CANDIDATES: usize = 16;

fn table_job_lock_key(table_oid: u32) -> i64 {
    (TABLE_JOB_LOCK_NAMESPACE << 32) | i64::from(table_oid)
}

/// A lockable job beyond the executor's first candidate page must make progress.
///
/// The first 16 table locks stay held until candidate 17 reaches a terminal
/// state. This catches fixed-page head-of-line blocking in queue selection.
#[tokio::test]
async fn seventeenth_candidate_is_not_starved_by_busy_first_page() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "candidate_page_starvation").await?;
    let dbname: String = db
        .client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);

    db.client
        .batch_execute(&format!(
            r#"
            ALTER DATABASE "{dbname}" SET koldstore.flush_execution = 'queue';
            ALTER DATABASE "{dbname}" SET koldstore.max_parallel_flush_jobs = 1;
            ALTER DATABASE "{dbname}" SET koldstore.min_max_rows_per_file = 1;
            SET koldstore.flush_execution = 'queue';
            SET koldstore.min_max_rows_per_file = 1;
            "#
        ))
        .await?;

    let mut relations = Vec::with_capacity(BUSY_CANDIDATES + 1);
    let mut table_oids = Vec::with_capacity(BUSY_CANDIDATES + 1);
    for index in 0..=BUSY_CANDIDATES {
        let table_name = format!("{}_candidate_{index:02}", db.schema);
        let table = db.create_indexed_items_table(&table_name, 16).await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                    table_name => $1::text::regclass,
                    storage => $2,
                    hot_row_limit => 2,
                    min_flush_rows => 1,
                    max_rows_per_file => 4,
                    migration_order_by => 'id',
                    auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        let oid: i64 = db
            .client
            .query_one("SELECT $1::text::regclass::oid::bigint", &[&table.relation])
            .await?
            .get(0);
        relations.push(table.relation);
        table_oids.push(u32::try_from(oid).context("table OID exceeds u32")?);
    }
    common::fence_async_mirror(&db.client).await?;

    let mut lock_holders = Vec::with_capacity(BUSY_CANDIDATES);
    for oid in &table_oids[..BUSY_CANDIDATES] {
        let peer = common::connect(&db.target).await?;
        let key = table_job_lock_key(*oid);
        peer.query_one("SELECT pg_advisory_lock($1::bigint)", &[&key])
            .await?;
        lock_holders.push((peer, key));
    }

    // Publish one queue generation only after all 17 jobs and their stable sort
    // order exist. Candidate 17 is deliberately the last row in that order.
    let mut enqueue_client = common::connect(&db.target).await?;
    let transaction = enqueue_client.transaction().await?;
    let mut job_ids = Vec::with_capacity(relations.len());
    for (index, relation) in relations.iter().enumerate() {
        let job_id: String = transaction
            .query_one(
                r#"
                SELECT koldstore.enqueue_flush_job(
                    table_name => $1::text::regclass,
                    force => true
                )::text
                "#,
                &[relation],
            )
            .await?
            .get(0);
        transaction
            .execute(
                r#"
                UPDATE koldstore.jobs
                SET available_at = now() - interval '1 minute'
                        + ($2::bigint * interval '1 millisecond'),
                    updated_at = now() - interval '1 minute'
                        + ($2::bigint * interval '1 millisecond')
                WHERE id = $1::text::uuid
                "#,
                &[&job_id, &i64::try_from(index).context("candidate index")?],
            )
            .await?;
        job_ids.push(job_id);
    }
    transaction.commit().await?;

    let seventeenth = &job_ids[BUSY_CANDIDATES];
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        common::wait_for_flush_job_terminal(&db.client, seventeenth),
    )
    .await;

    for (peer, key) in lock_holders {
        let _ = peer
            .query_one("SELECT pg_advisory_unlock($1::bigint)", &[&key])
            .await;
    }

    let flushed = result
        .map_err(|_| anyhow::anyhow!("candidate 17 starved behind the fixed 16-row busy page"))??;
    anyhow::ensure!(flushed > 0, "candidate 17 completed without flushing rows");

    db.client
        .batch_execute(&format!(
            r#"
            ALTER DATABASE "{dbname}" RESET koldstore.flush_execution;
            ALTER DATABASE "{dbname}" RESET koldstore.max_parallel_flush_jobs;
            ALTER DATABASE "{dbname}" RESET koldstore.min_max_rows_per_file;
            "#
        ))
        .await
        .ok();
    Ok(())
}
