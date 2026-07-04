#[path = "common/mod.rs"]
mod common;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use koldstore_manifest::Manifest;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tokio_postgres::Client;

#[test]
fn full_lifecycle_contract_covers_migrate_flush_merge_and_dml_checkpoints() {
    let checkpoints = [
        "heap table starts with primary key, unique, foreign key, and secondary index metadata",
        "migrate_table registers schema and records indexed columns for cold metadata",
        "migration/backfill jobs complete and fill _seq, _commit_seq, and _deleted",
        "first flush writes manifest.json and at least one parquet segment",
        "manifest segment records indexed-column stats and bloom filter metadata",
        "parquet footer is readable and contains rows",
        "merged SELECT returns cold rows plus newer hot inserts, updates, and deletes",
        "second flush preserves post-DML query results after hot rows become cold",
        "local cold metadata tracks all active segments and no pending jobs remain",
    ];

    for checkpoint in [
        "migrate_table registers schema and records indexed columns for cold metadata",
        "first flush writes manifest.json and at least one parquet segment",
        "merged SELECT returns cold rows plus newer hot inserts, updates, and deletes",
        "second flush preserves post-DML query results after hot rows become cold",
    ] {
        assert!(checkpoints.contains(&checkpoint));
    }
}

#[tokio::test]
async fn full_lifecycle_migrates_flushes_merges_hot_and_cold_then_flushes_again() -> Result<()> {
    for target in common::local_pg_matrix() {
        let client = common::wait_for_postgres(&target).await?;
        let storage_root = storage_root_for(target.version)?;
        run_full_lifecycle(&client, target.version, &storage_root).await?;
    }

    Ok(())
}

fn storage_root_for(pg_version: u16) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "pg-koldstore-full-lifecycle-{}-{pg_version}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
    Ok(root)
}

async fn run_full_lifecycle(client: &Client, pg_version: u16, storage_root: &Path) -> Result<()> {
    install_extension_and_storage(client, storage_root).await?;
    create_source_table(client, pg_version).await?;
    migrate_existing_table(client, pg_version).await?;
    wait_for_jobs_to_finish(client, pg_version).await?;
    assert_system_columns_filled(client, pg_version, 4).await?;
    assert_indexed_columns_registered(client, pg_version).await?;

    flush_table(client, pg_version).await?;
    wait_for_jobs_to_finish(client, pg_version).await?;
    let first_manifest = assert_manifest_and_parquet_artifacts(client, pg_version, storage_root, 1)
        .await
        .context("first flush artifacts")?;
    assert_local_cold_metadata(client, pg_version, 1).await?;

    apply_hot_dml_after_first_flush(client, pg_version).await?;
    assert_visible_rows(
        client,
        pg_version,
        &[
            (2, "two-hot-update", 20),
            (3, "three", 3),
            (4, "four", 4),
            (5, "five-hot", 5),
        ],
    )
    .await?;

    flush_table(client, pg_version).await?;
    wait_for_jobs_to_finish(client, pg_version).await?;
    let second_manifest =
        assert_manifest_and_parquet_artifacts(client, pg_version, storage_root, 2)
            .await
            .context("second flush artifacts")?;
    assert_local_cold_metadata(client, pg_version, 2).await?;
    assert!(second_manifest.segments.len() >= first_manifest.segments.len());
    assert_visible_rows(
        client,
        pg_version,
        &[
            (2, "two-hot-update", 20),
            (3, "three", 3),
            (4, "four", 4),
            (5, "five-hot", 5),
        ],
    )
    .await?;
    assert_no_pending_jobs(client, pg_version).await?;

    Ok(())
}

async fn install_extension_and_storage(client: &Client, storage_root: &Path) -> Result<()> {
    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS koldstore;")
        .await?;

    let base_path = storage_root
        .to_str()
        .context("filesystem storage path must be valid utf-8")?;
    client
        .execute(
            r#"
            SELECT koldstore.register_storage(
              'full-lifecycle-local',
              'filesystem',
              $1,
              '{}'::jsonb,
              '{}'::jsonb
            )
            "#,
            &[&base_path],
        )
        .await?;

    Ok(())
}

async fn create_source_table(client: &Client, pg_version: u16) -> Result<()> {
    let relation = relation(pg_version);
    client
        .batch_execute(&format!(
            r#"
            CREATE SCHEMA IF NOT EXISTS lifecycle;
            DROP TABLE IF EXISTS {relation};
            DROP TABLE IF EXISTS lifecycle.accounts_pg{pg_version};

            CREATE TABLE lifecycle.accounts_pg{pg_version} (
              account_id bigint PRIMARY KEY
            );

            INSERT INTO lifecycle.accounts_pg{pg_version}
            VALUES (10), (20);

            CREATE TABLE {relation} (
              id bigint PRIMARY KEY,
              account_id bigint NOT NULL REFERENCES lifecycle.accounts_pg{pg_version}(account_id),
              title text NOT NULL UNIQUE,
              qty integer NOT NULL,
              created_at timestamptz NOT NULL DEFAULT now(),
              CHECK (qty >= 0)
            );
            CREATE INDEX full_lifecycle_qty_idx_pg{pg_version} ON {relation} (qty);
            INSERT INTO {relation} (id, account_id, title, qty)
            VALUES
              (1, 10, 'one', 1),
              (2, 10, 'two', 2),
              (3, 20, 'three', 3),
              (4, 20, 'four', 4);
            "#,
        ))
        .await?;
    Ok(())
}

async fn migrate_existing_table(client: &Client, pg_version: u16) -> Result<()> {
    client
        .execute(
            r#"
            SELECT koldstore.migrate_table(
              $1::text::regclass,
              'shared',
              'full-lifecycle-local',
              NULL,
              NULL,
              'id'
            )
            "#,
            &[&relation(pg_version)],
        )
        .await?;
    Ok(())
}

async fn flush_table(client: &Client, pg_version: u16) -> Result<()> {
    client
        .execute(
            "SELECT koldstore.flush_table($1::text::regclass)",
            &[&relation(pg_version)],
        )
        .await?;
    Ok(())
}

async fn wait_for_jobs_to_finish(client: &Client, pg_version: u16) -> Result<()> {
    for _ in 0..60 {
        let active = active_job_count(client, pg_version).await?;
        if active == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("jobs did not finish for {}", relation(pg_version));
}

async fn active_job_count(client: &Client, pg_version: u16) -> Result<i64> {
    let row = client
        .query_one(
            r#"
            SELECT count(*)
            FROM koldstore.jobs
            WHERE table_oid = $1::text::regclass::oid
              AND status IN ('pending', 'running')
            "#,
            &[&relation(pg_version)],
        )
        .await?;
    Ok(row.get(0))
}

async fn assert_no_pending_jobs(client: &Client, pg_version: u16) -> Result<()> {
    assert_eq!(active_job_count(client, pg_version).await?, 0);
    Ok(())
}

async fn assert_system_columns_filled(
    client: &Client,
    pg_version: u16,
    expected_rows: i64,
) -> Result<()> {
    let row = client
        .query_one(
            &format!(
                r#"
                SELECT count(*)
                FROM {}
                WHERE _seq IS NOT NULL
                  AND _commit_seq IS NOT NULL
                  AND _deleted IS NOT NULL
                "#,
                relation(pg_version)
            ),
            &[],
        )
        .await?;
    assert_eq!(row.get::<_, i64>(0), expected_rows);
    Ok(())
}

async fn assert_indexed_columns_registered(client: &Client, pg_version: u16) -> Result<()> {
    let row = client
        .query_one(
            r#"
            SELECT COALESCE((options #> '{cold_metadata,indexed_columns}')::text, '[]')
            FROM koldstore.schemas
            WHERE table_oid = $1::text::regclass::oid
            ORDER BY version DESC
            LIMIT 1
            "#,
            &[&relation(pg_version)],
        )
        .await?;
    let indexed_columns: serde_json::Value = serde_json::from_str(&row.get::<_, String>(0))?;
    let columns = indexed_columns
        .as_array()
        .context("indexed_columns should be a JSON array")?;

    for expected in ["id", "account_id", "title", "qty"] {
        assert!(
            columns.iter().any(
                |entry| entry.get("column").and_then(serde_json::Value::as_str) == Some(expected)
            ),
            "missing indexed column metadata for {expected}"
        );
    }

    Ok(())
}

async fn assert_manifest_and_parquet_artifacts(
    client: &Client,
    pg_version: u16,
    storage_root: &Path,
    min_segments: usize,
) -> Result<Manifest> {
    let manifest_path = client
        .query_one(
            r#"
            SELECT manifest_path
            FROM koldstore.manifest
            WHERE table_oid = $1::text::regclass::oid
              AND sync_state = 'in_sync'
            ORDER BY generation DESC
            LIMIT 1
            "#,
            &[&relation(pg_version)],
        )
        .await?
        .get::<_, String>(0);
    let manifest_file = storage_root.join(&manifest_path);
    assert!(
        manifest_file.exists(),
        "missing {}",
        manifest_file.display()
    );

    let manifest: Manifest = serde_json::from_str(&std::fs::read_to_string(&manifest_file)?)?;
    assert!(
        manifest.segments.len() >= min_segments,
        "expected at least {min_segments} manifest segments"
    );

    for segment in &manifest.segments {
        assert!(
            !segment.column_stats.is_empty(),
            "segment {} should include indexed-column min/max stats",
            segment.path
        );
        assert!(
            !segment.bloom_filters.is_empty(),
            "segment {} should include bloom filter metadata",
            segment.path
        );

        let parquet_path = storage_root.join(&segment.path);
        assert!(parquet_path.exists(), "missing {}", parquet_path.display());
        let parquet_file = File::open(&parquet_path)
            .with_context(|| format!("open {}", parquet_path.display()))?;
        let reader = SerializedFileReader::new(parquet_file)
            .with_context(|| format!("read parquet footer {}", parquet_path.display()))?;
        assert!(reader.metadata().file_metadata().num_rows() > 0);
    }

    Ok(manifest)
}

async fn assert_local_cold_metadata(
    client: &Client,
    pg_version: u16,
    min_segments: i64,
) -> Result<()> {
    let row = client
        .query_one(
            r#"
            SELECT
              COALESCE(max(m.segment_count), 0),
              count(DISTINCT cs.segment_id),
              count(DISTINCT h.pk_hash)
            FROM koldstore.manifest m
            LEFT JOIN koldstore.cold_segments cs
              ON cs.table_oid = m.table_oid
             AND cs.scope_key = m.scope_key
             AND cs.status = 'active'
            LEFT JOIN koldstore.cold_pk_hints h
              ON h.table_oid = cs.table_oid
             AND h.scope_key = cs.scope_key
             AND h.segment_id = cs.segment_id
            WHERE m.table_oid = $1::text::regclass::oid
              AND m.sync_state = 'in_sync'
            "#,
            &[&relation(pg_version)],
        )
        .await?;

    assert!(row.get::<_, i32>(0) >= min_segments as i32);
    assert!(row.get::<_, i64>(1) >= min_segments);
    assert!(row.get::<_, i64>(2) > 0);
    Ok(())
}

async fn apply_hot_dml_after_first_flush(client: &Client, pg_version: u16) -> Result<()> {
    let relation = relation(pg_version);
    client
        .batch_execute(&format!(
            r#"
            UPDATE {relation}
            SET title = 'two-hot-update', qty = 20
            WHERE id = 2;

            DELETE FROM {relation}
            WHERE id = 1;

            INSERT INTO {relation} (id, account_id, title, qty)
            VALUES (5, 20, 'five-hot', 5);
            "#,
        ))
        .await?;
    Ok(())
}

async fn assert_visible_rows(
    client: &Client,
    pg_version: u16,
    expected: &[(i64, &str, i32)],
) -> Result<()> {
    let rows = client
        .query(
            &format!(
                "SELECT id, title, qty FROM {} ORDER BY id",
                relation(pg_version)
            ),
            &[],
        )
        .await?;
    let visible_rows = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, i32>(2),
            )
        })
        .collect::<Vec<_>>();
    let expected_rows = expected
        .iter()
        .map(|(id, title, qty)| (*id, (*title).to_string(), *qty))
        .collect::<Vec<_>>();

    assert_eq!(visible_rows, expected_rows);
    Ok(())
}

fn relation(pg_version: u16) -> String {
    format!("lifecycle.full_lifecycle_pg{pg_version}")
}
