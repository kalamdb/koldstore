//! Live `ALTER EXTENSION koldstore UPDATE` path (not just upgrade SQL file presence).
//!
//! Simulates an installed catalog at the previous packaged SQL version, then runs
//! `ALTER EXTENSION … UPDATE` against the currently installed shared library.
//! Full `pg_upgrade` remains deferred (see docs/operations/upgrade.md).

use crate::common;

use anyhow::{Context, Result};

const PREVIOUS_EXTENSION_SQL_VERSION: &str = "0.1.0";

#[tokio::test]
async fn alter_extension_update_preserves_managed_table() -> Result<()> {
    common::require_pgrx_server().await?;
    let mode = common::selected_mirror_capture_mode()?.as_str();

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "ext_upgrade").await?;

        let before: String = db
            .client
            .query_one(
                "SELECT extversion FROM pg_extension WHERE extname = 'koldstore'",
                &[],
            )
            .await
            .context("read current extversion")?
            .get(0);

        let table = db
            .create_indexed_items_table(&format!("upgrade_items_{}", db.schema), 12)
            .await?;
        let visible_before = common::relation_row_count(&db.client, &table.relation).await?;
        assert_eq!(visible_before, 12);
        db.client
            .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
            .await?;
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
                  auto_flush => false,
                  mirror_capture_mode => $3
                )
                "#,
                &[&table.relation, &db.storage_name, &mode],
            )
            .await?;
        let flushed = db.flush_table(&table.relation).await?;
        assert!(flushed > 0);

        // Pretend the catalog is still at the previous packaged SQL version so
        // ALTER EXTENSION UPDATE exercises the --from--to.sql edge. The loaded
        // .so is already current (pgrx install).
        db.client
            .execute(
                "UPDATE pg_catalog.pg_extension SET extversion = $1 WHERE extname = 'koldstore'",
                &[&PREVIOUS_EXTENSION_SQL_VERSION],
            )
            .await
            .context("simulate previous extversion")?;

        let simulated: String = db
            .client
            .query_one(
                "SELECT extversion FROM pg_extension WHERE extname = 'koldstore'",
                &[],
            )
            .await?
            .get(0);
        assert_eq!(simulated, PREVIOUS_EXTENSION_SQL_VERSION);

        db.client
            .batch_execute("ALTER EXTENSION koldstore UPDATE;")
            .await
            .context("ALTER EXTENSION koldstore UPDATE")?;

        let after: String = db
            .client
            .query_one(
                "SELECT extversion FROM pg_extension WHERE extname = 'koldstore'",
                &[],
            )
            .await?
            .get(0);
        assert_eq!(
            after, before,
            "extversion after UPDATE must return to packaged current ({before})"
        );

        let version_fn: String = db
            .client
            .query_one("SELECT koldstore_version()", &[])
            .await?
            .get(0);
        assert!(
            !version_fn.is_empty(),
            "koldstore_version() must work after UPDATE"
        );

        common::assert_catalog_has_active_schema(&db.client, &table.relation).await?;
        let visible = common::relation_row_count(&db.client, &table.relation).await?;
        assert_eq!(
            visible, visible_before,
            "managed rows must remain visible after UPDATE"
        );
        common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;

        // Post-upgrade flush still works.
        db.client
            .batch_execute(&format!(
                "INSERT INTO {} (id, account_id, title, qty, category) VALUES (100, 1, 'post-upgrade', 1, 'even')",
                table.relation
            ))
            .await?;
        let _ = db.flush_table(&table.relation).await?;
        let described = db
            .client
            .query_one(
                "SELECT koldstore.describe_table($1::text::regclass)::text",
                &[&table.relation],
            )
            .await?
            .get::<_, String>(0);
        assert!(
            described.contains("storage") || described.contains("mirror"),
            "describe_table should remain usable after UPDATE: {described}"
        );
    }

    Ok(())
}
