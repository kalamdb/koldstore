//! Shared post-crash / post-reclaim flush data-plane checks.
//!
//! After kill/restart + `recover_segments` + retry flush, assert the managed
//! table is queryable, hot/mirror agree with prune expectations, and cold
//! Parquet + `manifest.json` objects on disk match catalog metadata.

use std::path::Path;

use anyhow::{Context, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use tokio_postgres::Client;

use crate::common;

/// Expectations for [`assert_recovered_flush_data_plane`].
pub struct RecoveredFlushExpect<'a> {
    /// Visible merged row count (`SELECT count(*)` on the managed relation).
    pub visible_rows: i64,
    /// When true, every live row must be cold (hot heap + `__cl` empty).
    pub expect_hot_fully_pruned: bool,
    /// Minimum active cold segments after recovery (multi-pass flush → >1).
    pub min_cold_segments: i64,
    /// Optional heap twin for content equality (`relation`, order/compare cols).
    pub reference: Option<(&'a str, &'a [&'a str])>,
}

/// Asserts mirror/hot/query/catalog/Parquet/manifest consistency after crash recovery.
///
/// # Errors
///
/// Returns an error when any data-plane check fails.
pub async fn assert_recovered_flush_data_plane(
    client: &Client,
    relation: &str,
    storage_root: &Path,
    expect: RecoveredFlushExpect<'_>,
) -> Result<()> {
    common::fence_async_mirror(client).await?;
    common::assert_no_active_jobs(client, relation).await?;
    common::assert_pk_unique(client, relation, &["id"]).await?;

    let visible = common::relation_row_count(client, relation).await?;
    anyhow::ensure!(
        visible == expect.visible_rows,
        "visible row count for {relation}: expected {}, got {visible}",
        expect.visible_rows
    );

    if expect.expect_hot_fully_pruned {
        common::assert_flush_pruned_hot_storage(client, relation, expect.visible_rows).await?;
    } else {
        let mirror = common::change_log_mirror_relation(relation);
        let hot = common::hot_row_count(client, relation).await?;
        let mirror_rows = common::row_count(client, &mirror).await?;
        anyhow::ensure!(
            hot == mirror_rows,
            "hot heap ({hot}) and mirror {mirror} ({mirror_rows}) must agree after recovery"
        );
    }

    common::assert_cold_metadata_present(client, relation).await?;
    let published = common::published_manifest_count(client, relation).await?;
    anyhow::ensure!(
        published >= 1,
        "expected at least one published in_sync manifest for {relation}, got {published}"
    );

    let integrity_text: String = client
        .query_one(
            "SELECT koldstore.verify_table_integrity($1::text::regclass)::text",
            &[&relation],
        )
        .await
        .context("verify_table_integrity")?
        .get(0);
    let integrity: serde_json::Value =
        serde_json::from_str(&integrity_text).context("parse verify_table_integrity json")?;
    anyhow::ensure!(
        integrity.get("ok").and_then(|v| v.as_bool()) == Some(true),
        "verify_table_integrity reported failure: {integrity}"
    );

    assert_cold_objects_on_disk(client, relation, storage_root, expect.min_cold_segments).await?;

    // Sample query path: ordered PK probe must see every live id via merge scan.
    let ids = client
        .query(&format!("SELECT id FROM {relation} ORDER BY id"), &[])
        .await
        .with_context(|| format!("ordered SELECT after recovery on {relation}"))?;
    anyhow::ensure!(
        i64::try_from(ids.len()).unwrap_or(i64::MAX) == expect.visible_rows,
        "ordered SELECT returned {} rows, expected {}",
        ids.len(),
        expect.visible_rows
    );

    if let Some((reference, cols)) = expect.reference {
        common::assert_managed_matches_reference_ordered(client, relation, reference, cols).await?;
    }

    Ok(())
}

/// Loads catalog cold paths and asserts each Parquet object + manifest exist and decode.
async fn assert_cold_objects_on_disk(
    client: &Client,
    relation: &str,
    storage_root: &Path,
    min_cold_segments: i64,
) -> Result<()> {
    let rows = client
        .query(
            &format!(
                r#"
                SELECT
                  {manifest} AS manifest_key,
                  {object} AS object_key,
                  cs.row_count,
                  cs.path
                FROM koldstore.manifest m
                JOIN pg_class c ON c.oid = m.table_oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN koldstore.cold_segments cs
                  ON cs.table_oid = m.table_oid
                 AND cs.scope_key = m.scope_key
                WHERE m.table_oid = $1::text::regclass::oid
                  AND m.generation > 0
                  AND m.sync_state = 'in_sync'
                  AND cs.status = 'active'
                ORDER BY cs.batch_number
                "#,
                manifest = common::SQL_DEFAULT_MANIFEST_OBJECT_KEY,
                object = common::SQL_DEFAULT_COLD_OBJECT_KEY,
            ),
            &[&relation],
        )
        .await
        .context("list active cold segment objects")?;

    anyhow::ensure!(
        i64::try_from(rows.len()).unwrap_or(0) >= min_cold_segments,
        "expected at least {min_cold_segments} active cold segments for {relation}, got {}",
        rows.len()
    );

    let mut parquet_rows = 0_i64;
    let mut seen_manifest = false;
    for row in &rows {
        let manifest_key: String = row.get(0);
        let object_key: String = row.get(1);
        let row_count: i64 = row.get(2);
        let relative_path: String = row.get(3);

        let manifest_path = storage_root.join(&manifest_key);
        let parquet_path = storage_root.join(&object_key);
        anyhow::ensure!(
            manifest_path.exists(),
            "missing cold manifest {}",
            manifest_path.display()
        );
        anyhow::ensure!(
            parquet_path.exists(),
            "missing cold parquet {}",
            parquet_path.display()
        );
        anyhow::ensure!(
            relative_path.ends_with(".parquet"),
            "cold_segments.path must be a parquet relative path, got {relative_path}"
        );

        if !seen_manifest {
            let loaded = koldstore_manifest::try_load_manifest_from_path(&manifest_path)
                .map_err(|error| anyhow::anyhow!(error))
                .with_context(|| format!("decode manifest {}", manifest_path.display()))?
                .with_context(|| format!("manifest missing at {}", manifest_path.display()))?;
            anyhow::ensure!(
                !loaded.segments.is_empty(),
                "manifest {} has no segments",
                manifest_path.display()
            );
            seen_manifest = true;
        }

        let file = std::fs::File::open(&parquet_path)
            .with_context(|| format!("open {}", parquet_path.display()))?;
        let reader = SerializedFileReader::new(file)
            .with_context(|| format!("read parquet {}", parquet_path.display()))?;
        let file_rows = reader.metadata().file_metadata().num_rows();
        anyhow::ensure!(
            file_rows == row_count,
            "parquet {} row_count catalog={row_count} file={file_rows}",
            parquet_path.display()
        );
        parquet_rows = parquet_rows.saturating_add(file_rows);
    }

    anyhow::ensure!(
        parquet_rows > 0,
        "expected readable parquet rows for {relation}"
    );
    Ok(())
}
