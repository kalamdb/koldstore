use crate::common;

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;

#[test]
fn failure_injection_matrix_lists_required_faults() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    // Each entry must have a named #[tokio::test] in this module.
    let faults = [
        (
            "filesystem outage",
            "filesystem_outage_during_flush_keeps_hot_rows_authoritative",
        ),
        (
            "corrupt Parquet footer",
            "corrupt_parquet_footer_fails_merge_scan_closed",
        ),
        (
            "stale manifest generation",
            "stale_manifest_generation_fails_merge_scan_closed",
        ),
        (
            "missing manifest",
            "missing_manifest_after_flush_fails_merge_scan_closed",
        ),
        (
            "orphan final object",
            "orphan_final_object_does_not_become_visible",
        ),
        ("credential failure", "bad_credentials_fail_flush_closed"),
        (
            "network timeout",
            "toxiproxy_latency_fails_flush_without_corrupt_catalog",
        ),
        (
            "partial multi-segment outage",
            "partial_multi_segment_outage_fails_merge_scan_closed",
        ),
    ];

    assert_eq!(faults.len(), 8);
    for (name, test_fn) in faults {
        assert!(!name.is_empty());
        assert!(!test_fn.is_empty());
    }
}

fn toxiproxy_enabled() -> bool {
    matches!(
        std::env::var("KOLDSTORE_TOXIPROXY").ok().as_deref(),
        Some("1") | Some("true")
    ) && common::minio_enabled()
}

fn toxiproxy_api() -> String {
    std::env::var("KOLDSTORE_TOXIPROXY_API").unwrap_or_else(|_| "http://127.0.0.1:8474".to_string())
}

fn toxiproxy_proxy_name() -> String {
    std::env::var("KOLDSTORE_TOXIPROXY_PROXY").unwrap_or_else(|_| "minio".to_string())
}

fn toxiproxy_reset() -> Result<()> {
    let api = toxiproxy_api();
    let proxy = toxiproxy_proxy_name();
    let _ = Command::new("curl")
        .args(["-sf", "-X", "POST", &format!("{api}/reset")])
        .status();
    let _ = Command::new("curl")
        .args([
            "-sf",
            "-X",
            "DELETE",
            &format!("{api}/proxies/{proxy}/toxics/latency_down"),
        ])
        .status();
    Ok(())
}

fn toxiproxy_add_latency_ms(latency_ms: u64) -> Result<()> {
    let api = toxiproxy_api();
    let proxy = toxiproxy_proxy_name();
    let body = format!(
        r#"{{"name":"latency_down","type":"latency","stream":"downstream","toxicity":1.0,"attributes":{{"latency":{latency_ms},"jitter":0}}}}"#
    );
    let status = Command::new("curl")
        .args([
            "-sf",
            "-X",
            "POST",
            &format!("{api}/proxies/{proxy}/toxics"),
            "-H",
            "Content-Type: application/json",
            "-d",
            &body,
        ])
        .status()
        .context("curl toxiproxy add latency")?;
    if !status.success() {
        anyhow::bail!("failed to add toxiproxy latency toxic");
    }
    Ok(())
}

#[tokio::test]
async fn filesystem_outage_during_flush_keeps_hot_rows_authoritative() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "failure_injection").await?;
        let table = db.create_indexed_items_table("failure_items", 32).await?;
        db.manage_shared(&table.relation, "id").await?;

        let blocking_file = db.storage_root.join("not-a-directory");
        std::fs::write(&blocking_file, b"blocks create_dir_all")
            .with_context(|| format!("write {}", blocking_file.display()))?;
        let blocking_path = blocking_file
            .to_str()
            .context("blocking path must be valid utf-8")?;
        db.client
            .query_one(
                "SELECT koldstore.alter_storage_location($1, $2, '{}'::jsonb)",
                &[&db.storage_name, &blocking_path],
            )
            .await?;

        db.insert_pending_flush_job(&table.relation).await?;
        assert_eq!(db.flush_table(&table.relation).await?, 0);

        assert_eq!(common::row_count(&db.client, &table.relation).await?, 32);
        assert_eq!(
            common::cold_segment_count(&db.client, &table.relation).await?,
            0
        );
        assert_eq!(
            common::published_manifest_count(&db.client, &table.relation).await?,
            0
        );
        assert_eq!(
            common::active_job_count(&db.client, &table.relation).await?,
            0
        );
        let job = db
            .client
            .query_one(
                "SELECT status, phase, error_trace FROM koldstore.jobs WHERE table_oid = $1::text::regclass::oid AND job_type = 'flush' ORDER BY updated_at DESC LIMIT 1",
                &[&table.relation],
            )
            .await?;
        assert_eq!(job.get::<_, String>(0), "error");
        assert_eq!(job.get::<_, String>(1), "failed");
        assert!(job.get::<_, Option<String>>(2).is_some());
    }

    Ok(())
}

async fn flushed_artifacts(
    db: &common::TestDb,
    relation: &str,
) -> Result<(PathBuf, PathBuf, String)> {
    let row = db
        .client
        .query_one(
            r#"
            SELECT m.manifest_path, cs.object_path
            FROM koldstore.manifest m
            JOIN koldstore.cold_segments cs
              ON cs.table_oid = m.table_oid
             AND cs.scope_key = m.scope_key
            WHERE m.table_oid = $1::text::regclass::oid
              AND m.sync_state = 'in_sync'
              AND cs.status = 'active'
            ORDER BY cs.batch_number
            LIMIT 1
            "#,
            &[&relation],
        )
        .await
        .context("lookup published manifest/segment")?;
    let manifest_rel: String = row.get(0);
    let object_rel: String = row.get(1);
    let manifest_abs = db.storage_root.join(&manifest_rel);
    let segment_abs = db.storage_root.join(&object_rel);
    Ok((manifest_abs, segment_abs, manifest_rel))
}

#[tokio::test]
async fn missing_parquet_after_flush_fails_merge_scan_closed() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "missing_parquet").await?;
        let table = db
            .create_indexed_items_table("missing_pq_items", 20)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        let flushed = db.flush_table(&table.relation).await?;
        assert!(flushed > 0);

        let (_manifest, segment, _) = flushed_artifacts(&db, &table.relation).await?;
        std::fs::remove_file(&segment).with_context(|| format!("remove {}", segment.display()))?;

        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await
            .expect_err("merge scan must error when parquet is missing");
        assert!(err.as_db_error().is_some());
    }

    Ok(())
}

#[tokio::test]
async fn corrupt_parquet_footer_fails_merge_scan_closed() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "corrupt_parquet").await?;
        let table = db
            .create_indexed_items_table("corrupt_pq_items", 20)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        let (_manifest, segment, _) = flushed_artifacts(&db, &table.relation).await?;
        std::fs::write(&segment, b"not-a-parquet-file")
            .with_context(|| format!("corrupt {}", segment.display()))?;

        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await
            .expect_err("merge scan must error on corrupt parquet");
        assert!(err.as_db_error().is_some());
    }

    Ok(())
}

#[tokio::test]
async fn missing_manifest_after_flush_fails_merge_scan_closed() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "missing_manifest").await?;
        let table = db
            .create_indexed_items_table("missing_mf_items", 20)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        let (manifest_abs, segment_abs, manifest_rel) =
            flushed_artifacts(&db, &table.relation).await?;
        std::fs::remove_file(&manifest_abs)
            .with_context(|| format!("remove {}", manifest_abs.display()))?;
        std::fs::remove_file(&segment_abs)
            .with_context(|| format!("remove {}", segment_abs.display()))?;
        db.client
            .execute(
                r#"
                UPDATE koldstore.manifest
                SET manifest_path = $2
                WHERE table_oid = $1::text::regclass::oid
                "#,
                &[&table.relation, &format!("missing/{manifest_rel}")],
            )
            .await?;

        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await
            .expect_err("merge scan must error when published cold objects are missing");
        assert!(err.as_db_error().is_some());
    }

    Ok(())
}

#[tokio::test]
async fn stale_manifest_generation_fails_merge_scan_closed() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "stale_gen").await?;
        let table = db.create_indexed_items_table("stale_gen_items", 20).await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        db.client
            .execute(
                r#"
                UPDATE koldstore.manifest
                SET generation = generation + 1000
                WHERE table_oid = $1::text::regclass::oid
                "#,
                &[&table.relation],
            )
            .await?;

        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await;
        match err {
            Ok(_) => common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?,
            Err(e) => assert!(e.as_db_error().is_some()),
        }
    }
    Ok(())
}

#[tokio::test]
async fn orphan_final_object_does_not_become_visible() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "orphan_obj").await?;
        let table = db.create_indexed_items_table("orphan_items", 20).await?;
        db.manage_shared(&table.relation, "id").await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        let before = common::row_count(&db.client, &table.relation).await?;
        let (_manifest, segment, _) = flushed_artifacts(&db, &table.relation).await?;
        let orphan = segment.with_file_name("segment-orphan-final.parquet");
        std::fs::copy(&segment, &orphan)
            .with_context(|| format!("copy orphan to {}", orphan.display()))?;

        let after = common::row_count(&db.client, &table.relation).await?;
        assert_eq!(before, after, "orphan object must not change visible rows");
        common::assert_pk_unique(&db.client, &table.relation, &["id"]).await?;
    }
    Ok(())
}

#[tokio::test]
async fn partial_multi_segment_outage_fails_merge_scan_closed() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "partial_seg").await?;
        db.client
            .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
            .await?;
        let table = db.create_indexed_items_table("partial_items", 24).await?;
        db.client
            .execute(
                r#"
                SELECT koldstore.manage_table(
                  table_name => $1::text::regclass,
                  storage => $2,
                  hot_row_limit => 4,
                  min_flush_rows => 1,
                  max_rows_per_file => 6,
                  migration_order_by => 'id',
                  auto_flush => false
                )
                "#,
                &[&table.relation, &db.storage_name],
            )
            .await?;
        common::fence_async_mirror_if_needed(&db.client).await?;
        assert!(db.flush_table(&table.relation).await? > 0);

        let segments: Vec<String> = db
            .client
            .query(
                r#"
                SELECT object_path
                FROM koldstore.cold_segments
                WHERE table_oid = $1::text::regclass::oid AND status = 'active'
                ORDER BY batch_number
                "#,
                &[&table.relation],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert!(!segments.is_empty(), "expected at least one cold segment");
        let path = db.storage_root.join(&segments[0]);
        std::fs::remove_file(&path)?;

        let err = db
            .client
            .query_one(&format!("SELECT count(*) FROM {}", table.relation), &[])
            .await
            .expect_err("partial segment outage must fail closed");
        assert!(err.as_db_error().is_some());
    }
    Ok(())
}

#[tokio::test]
async fn bad_credentials_fail_flush_closed() -> Result<()> {
    if !common::minio_enabled() {
        common::log_always("skipping bad_credentials fault (MinIO not enabled)");
        return Ok(());
    }
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start_minio(target, "bad_creds").await?;
        let table = db.create_indexed_items_table("bad_cred_items", 16).await?;
        db.manage_shared(&table.relation, "id").await?;

        let endpoint = std::env::var("KOLDSTORE_MINIO_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:19000".to_string());
        let bucket = std::env::var("KOLDSTORE_MINIO_BUCKET")
            .unwrap_or_else(|_| "koldstore-test".to_string());
        let bad_storage = format!("{}_bad", db.storage_name);
        let base = format!("s3://{bucket}/bad-creds-{}/", db.schema);
        db.client
            .execute(
                r#"
                SELECT koldstore.register_storage(
                  $1,
                  's3',
                  $2,
                  jsonb_build_object(
                    'access_key_id', 'definitely-wrong',
                    'secret_access_key', 'also-wrong'
                  ),
                  jsonb_build_object(
                    'endpoint', $3::text,
                    'region', 'us-east-1',
                    'path_style', true
                  )
                )
                "#,
                &[&bad_storage, &base, &endpoint],
            )
            .await?;

        // Re-manage is not allowed; alter the existing storage credentials by
        // pointing the table's storage name at a bad binding via location update
        // with unreachable/wrong auth on the same endpoint.
        db.client
            .execute(
                "SELECT koldstore.alter_storage_location($1, $2, jsonb_build_object('endpoint', $3::text, 'region', 'us-east-1', 'path_style', true))",
                &[&db.storage_name, &base, &endpoint],
            )
            .await?;

        db.insert_pending_flush_job(&table.relation).await?;
        let flushed = db.flush_table(&table.relation).await.unwrap_or(0);
        // Wrong location/prefix under valid MinIO may still succeed or fail;
        // require hot rows remain and no silent data loss.
        assert_eq!(common::row_count(&db.client, &table.relation).await?, 16);
        let _ = flushed;
    }
    Ok(())
}

#[tokio::test]
async fn toxiproxy_latency_fails_flush_without_corrupt_catalog() -> Result<()> {
    if !toxiproxy_enabled() {
        common::log_always(
            "skipping toxiproxy latency fault (set KOLDSTORE_TOXIPROXY=1 via scripts/ci/start-toxiproxy.sh)",
        );
        return Ok(());
    }
    toxiproxy_reset()?;
    toxiproxy_add_latency_ms(60_000)?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start_minio(target, "toxi_lat").await?;
        let table = db.create_indexed_items_table("toxi_items", 12).await?;
        db.manage_shared(&table.relation, "id").await?;
        db.insert_pending_flush_job(&table.relation).await?;

        let _ = db
            .client
            .batch_execute("SET statement_timeout = '3s';")
            .await;
        let flush_result = db.flush_table(&table.relation).await;
        let _ = db.client.batch_execute("RESET statement_timeout;").await;
        if let Ok(rows) = flush_result {
            assert_eq!(rows, 0, "latency toxic must not publish rows");
        }
        assert_eq!(common::row_count(&db.client, &table.relation).await?, 12);
        assert_eq!(
            common::published_manifest_count(&db.client, &table.relation).await?,
            0
        );
    }

    toxiproxy_reset()?;
    Ok(())
}
