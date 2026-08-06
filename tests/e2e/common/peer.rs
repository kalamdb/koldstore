//! Shared multi-session peer connections and advisory barriers for E2E tests.

use anyhow::{Context, Result};
use tokio_postgres::Client;

use super::TestDb;

/// Advisory-lock namespace for failpoint barriers (`"KOLD"` as i32).
///
/// Must match `pg_koldstore::failpoints::FAILPOINT_BARRIER_NAMESPACE`. The
/// second lock key is the current database OID so parallel worker DBs isolate.
pub const BARRIER_LOCK_NAMESPACE: i32 = 0x4B4F_4C44;

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

/// Opens a peer that runs Nested/`inline` flush in the calling backend.
///
/// Production default is `flush_execution=queue`. Failpoint-driven flush tests
/// need the peer itself to execute flush work.
///
/// # Errors
///
/// Returns an error when the connection or GUC SET fails.
pub async fn connect_flush_peer(db: &TestDb) -> Result<Client> {
    let client = connect_peer(db).await?;
    client
        .batch_execute("SET koldstore.flush_execution = 'inline'")
        .await
        .context("peer SET flush_execution=inline")?;
    Ok(client)
}

/// Resolves `current_database()` OID for the per-DB failpoint barrier key.
async fn current_database_oid(client: &Client) -> Result<i32> {
    let oid: i64 = client
        .query_one(
            "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = current_database()",
            &[],
        )
        .await
        .context("resolve current_database oid")?
        .get(0);
    i32::try_from(oid).context("database oid does not fit advisory lock int4 key")
}

/// Acquires the per-database flush/isolation barrier lock (blocks until available).
///
/// # Errors
///
/// Returns an error when PostgreSQL rejects the lock call.
pub async fn barrier_lock(client: &Client) -> Result<()> {
    let database_oid = current_database_oid(client).await?;
    client
        .execute(
            "SELECT pg_advisory_lock($1, $2)",
            &[&BARRIER_LOCK_NAMESPACE, &database_oid],
        )
        .await?;
    Ok(())
}

/// Releases the per-database flush/isolation barrier lock.
///
/// # Errors
///
/// Returns an error when unlock fails.
pub async fn barrier_unlock(client: &Client) -> Result<()> {
    let database_oid = current_database_oid(client).await?;
    client
        .execute(
            "SELECT pg_advisory_unlock($1, $2)",
            &[&BARRIER_LOCK_NAMESPACE, &database_oid],
        )
        .await?;
    Ok(())
}
