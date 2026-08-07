//! CREATE / DROP EXTENSION round-trip coverage.
//!
//! Packaging version contracts live in `crates/pg_koldstore/tests/extension_upgrade.rs`.
//! This module exercises live install, CASCADE uninstall with managed tables, and reinstall.

use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::common;

#[tokio::test]
async fn drop_extension_requires_cascade_with_managed_tables_then_reinstalls() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "ext_lifecycle").await?;
        let table = db
            .create_indexed_items_table("ext_lifecycle_items", 8)
            .await?;
        db.manage_shared(&table.relation, "id").await?;

        let present: bool = db
            .client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'koldstore')",
                &[],
            )
            .await?
            .get(0);
        assert!(present, "koldstore must be installed before DROP");

        // Quiesce managed-table workers before any DROP attempt (same pattern as
        // cluster::sync_koldstore_extension_sql). Without this, DROP can deadlock
        // against the async applier's AccessShare locks instead of returning the
        // dependent-object / CASCADE error the test asserts.
        quiesce_extension_drop_targets(&db.client).await?;

        let message = drop_extension_without_cascade_error(&db.client)
            .await
            .context("DROP EXTENSION without CASCADE")?;
        anyhow::ensure!(
            message.to_ascii_lowercase().contains("cascade")
                || message.to_ascii_lowercase().contains("depend"),
            "expected dependent-object / CASCADE error, got: {message}"
        );

        db.client
            .batch_execute("DROP EXTENSION koldstore CASCADE;")
            .await
            .context("DROP EXTENSION CASCADE")?;

        let gone: bool = db
            .client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'koldstore')",
                &[],
            )
            .await?
            .get(0);
        assert!(!gone, "extension must be absent after CASCADE drop");

        db.client
            .batch_execute("CREATE EXTENSION koldstore;")
            .await
            .context("CREATE EXTENSION after CASCADE drop")?;

        let version: String = db
            .client
            .query_one("SELECT koldstore_version()", &[])
            .await
            .context("call koldstore_version after reinstall")?
            .get(0);
        assert!(
            !version.is_empty(),
            "reinstalled extension must report a version"
        );

        // CASCADE drops catalog storage rows; re-register and manage a fresh table.
        let root = db.storage_root.display().to_string();
        db.client
            .execute(
                r#"
                SELECT koldstore.register_storage(
                  $1,
                  'filesystem',
                  $2,
                  '{}'::jsonb,
                  '{}'::jsonb
                )
                "#,
                &[&db.storage_name, &root],
            )
            .await
            .context("re-register filesystem storage after extension reinstall")?;
        let again = db
            .create_indexed_items_table("ext_lifecycle_reinstall", 4)
            .await?;
        db.manage_shared(&again.relation, "id").await?;
        assert_eq!(common::row_count(&db.client, &again.relation).await?, 4);
    }

    Ok(())
}

async fn quiesce_extension_drop_targets(client: &tokio_postgres::Client) -> Result<()> {
    let _ = client
        .batch_execute("UPDATE koldstore.schemas SET active = false WHERE active;")
        .await;
    let _ = common::terminate_async_worker(client).await;
    // Brief settle so terminated backends release relation locks before DROP.
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

/// Runs `DROP EXTENSION koldstore` and returns the error text.
///
/// Retries after re-quiescing when Postgres reports a transient deadlock instead
/// of the stable dependent-object failure.
async fn drop_extension_without_cascade_error(client: &tokio_postgres::Client) -> Result<String> {
    let mut last_message = String::new();
    for attempt in 1..=4 {
        match client.batch_execute("DROP EXTENSION koldstore;").await {
            Ok(()) => bail!(
                "DROP EXTENSION without CASCADE must fail while managed tables/dependents exist"
            ),
            Err(error) => {
                let _ = client.batch_execute("ROLLBACK").await;
                let message = error
                    .as_db_error()
                    .map(|db| db.message().to_string())
                    .unwrap_or_else(|| format!("{error:?}"));
                let lower = message.to_ascii_lowercase();
                if lower.contains("deadlock detected") || lower.contains("canceling statement") {
                    last_message = message;
                    quiesce_extension_drop_targets(client).await?;
                    tokio::time::sleep(Duration::from_millis(25 * attempt as u64)).await;
                    continue;
                }
                return Ok(message);
            }
        }
    }
    bail!("DROP EXTENSION without CASCADE kept deadlocking: {last_message}")
}

#[tokio::test]
async fn create_extension_is_idempotent_with_if_not_exists() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "ext_idempotent").await?;
        db.client
            .batch_execute("CREATE EXTENSION IF NOT EXISTS koldstore;")
            .await
            .context("CREATE EXTENSION IF NOT EXISTS (already installed)")?;
        let version: String = db
            .client
            .query_one("SELECT koldstore_version()", &[])
            .await?
            .get(0);
        assert!(!version.is_empty());
    }

    Ok(())
}
