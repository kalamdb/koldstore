//! Deterministic multi-session isolation harness for flush/DML schedules.
//!
//! Two `tokio-postgres` clients coordinate with PostgreSQL advisory locks.
//! Correctness must not depend on sleeps.

use anyhow::{Context, Result};
use tokio_postgres::Client;

use crate::common::{self, TestDb};

/// Well-known advisory lock used as a barrier between sessions.
pub const BARRIER_LOCK_KEY: i64 = 0x4B4F_4C44; // "KOLD"

/// Opens a second client against the same pgrx database as `db`.
///
/// # Errors
///
/// Returns an error when the connection fails.
pub async fn connect_peer(db: &TestDb) -> Result<Client> {
    let (client, connection) =
        tokio_postgres::connect(&db.target.connection_string(), tokio_postgres::NoTls)
            .await
            .context("connect peer client")?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("peer connection error: {error}");
        }
    });
    Ok(client)
}

/// Acquires the shared isolation barrier lock (blocks until available).
///
/// # Errors
///
/// Returns an error when PostgreSQL rejects the lock call.
pub async fn barrier_lock(client: &Client) -> Result<()> {
    client
        .execute("SELECT pg_advisory_lock($1)", &[&BARRIER_LOCK_KEY])
        .await?;
    Ok(())
}

/// Releases the shared isolation barrier lock.
///
/// # Errors
///
/// Returns an error when unlock fails.
pub async fn barrier_unlock(client: &Client) -> Result<()> {
    client
        .execute("SELECT pg_advisory_unlock($1)", &[&BARRIER_LOCK_KEY])
        .await?;
    Ok(())
}

/// Seeds a managed items table and returns its relation name.
///
/// # Errors
///
/// Returns an error when fixture setup fails.
pub async fn seed_managed_items(db: &TestDb, table: &str, rows: i64) -> Result<String> {
    let managed = db.create_indexed_items_table(table, rows).await?;
    db.client
        .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 8,
              min_flush_rows => 1,
              max_rows_per_file => 16
            )
            "#,
            &[&managed.relation, &db.storage_name],
        )
        .await?;
    Ok(managed.relation)
}

/// Asserts visible row values match a plain baseline relation.
///
/// Compares business columns only (`id`, `account_id`, `title`, `qty`, `category`)
/// so `created_at` clock skew cannot false-fail the schedule.
///
/// # Errors
///
/// Returns an error when equality fails.
pub async fn assert_matches_baseline(client: &Client, baseline: &str, managed: &str) -> Result<()> {
    let left_only = client
        .query(
            &format!(
                r#"
                SELECT id, account_id, title, qty, category FROM {managed}
                EXCEPT ALL
                SELECT id, account_id, title, qty, category FROM {baseline}
                "#
            ),
            &[],
        )
        .await?;
    let right_only = client
        .query(
            &format!(
                r#"
                SELECT id, account_id, title, qty, category FROM {baseline}
                EXCEPT ALL
                SELECT id, account_id, title, qty, category FROM {managed}
                "#
            ),
            &[],
        )
        .await?;
    if !left_only.is_empty() || !right_only.is_empty() {
        anyhow::bail!(
            "baseline mismatch: managed-only={} baseline-only={}",
            left_only.len(),
            right_only.len()
        );
    }
    common::assert_pk_unique(client, managed, &["id"]).await?;
    Ok(())
}

/// Creates a plain-heap baseline mirroring `managed` content (id, title, qty only).
///
/// # Errors
///
/// Returns an error when DDL/DML fails.
pub async fn mirror_baseline(client: &Client, schema: &str, managed: &str) -> Result<String> {
    let baseline = format!("{schema}_iso_baseline");
    let qualified = format!("{schema}.{baseline}");
    client
        .batch_execute(&format!(
            r#"
            DROP TABLE IF EXISTS {qualified};
            CREATE TABLE {qualified} AS
            SELECT id, account_id, title, qty, category, created_at FROM {managed};
            ALTER TABLE {qualified} ADD PRIMARY KEY (id);
            "#
        ))
        .await?;
    Ok(qualified)
}
