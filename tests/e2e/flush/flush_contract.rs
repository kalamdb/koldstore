//! Fail-closed SQL contracts required by WAL-only capture.

use anyhow::{Context, Result};

use crate::common;

/// TRUNCATE has no row-level logical changes for the mirror decoder, so every
/// form touching a managed table must fail atomically before utility execution.
#[tokio::test]
async fn truncate_is_rejected_before_and_after_cold_publication() -> Result<()> {
    common::require_pgrx_server().await?;
    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("PostgreSQL target")?;
    let db = common::TestDb::start(target, "truncate_rejected").await?;
    let table_name = format!("{}_items", db.schema);
    let table = db.create_indexed_items_table(&table_name, 100).await?;
    db.client
        .batch_execute("SET koldstore.min_max_rows_per_file = 1")
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
                table_name => $1::text::regclass,
                storage => $2,
                hot_row_limit => 10,
                min_flush_rows => 1,
                max_rows_per_file => 10,
                migration_order_by => 'id',
                auto_flush => false
            )
            "#,
            &[&table.relation, &db.storage_name],
        )
        .await?;
    common::fence_async_mirror(&db.client).await?;

    for statement in [
        format!("TRUNCATE TABLE {}", table.relation),
        format!("TRUNCATE ONLY {}", table.relation),
        format!("TRUNCATE TABLE {} RESTART IDENTITY", table.relation),
    ] {
        anyhow::ensure!(
            db.client.batch_execute(&statement).await.is_err(),
            "managed statement must fail closed: {statement}"
        );
        anyhow::ensure!(common::relation_row_count(&db.client, &table.relation).await? == 100);
    }

    let unmanaged = db.relation("truncate_unmanaged");
    db.client
        .batch_execute(&format!(
            "CREATE TABLE {unmanaged} (id bigint); INSERT INTO {unmanaged} VALUES (1)"
        ))
        .await?;
    let mixed = format!("TRUNCATE TABLE {unmanaged}, {}", table.relation);
    anyhow::ensure!(
        db.client.batch_execute(&mixed).await.is_err(),
        "mixed managed/unmanaged TRUNCATE must reject the whole statement"
    );
    anyhow::ensure!(common::relation_row_count(&db.client, &unmanaged).await? == 1);

    let flushed = db.flush_table_with_force(&table.relation, true).await?;
    anyhow::ensure!(flushed > 0);
    let before = common::relation_row_count(&db.client, &table.relation).await?;
    anyhow::ensure!(
        db.client
            .batch_execute(&format!("TRUNCATE TABLE {} CASCADE", table.relation))
            .await
            .is_err(),
        "managed TRUNCATE after cold publication must fail closed"
    );
    anyhow::ensure!(common::relation_row_count(&db.client, &table.relation).await? == before);
    common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;
    Ok(())
}
