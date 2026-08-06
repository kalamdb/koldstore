//! Cancel mid-read coverage for managed-table queries.
//!
//! Job cancel lives in `flush/flush_cancel_and_drop.rs`. This module covers
//! statement cancel while a merge scan is emitting rows, plus optional
//! Toxiproxy latency on cold object-store GETs.

use std::process::Command;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio_postgres::error::SqlState;

use crate::common;

fn is_query_canceled(error: &tokio_postgres::Error) -> bool {
    error
        .code()
        .is_some_and(|code| *code == SqlState::QUERY_CANCELED)
        || error.to_string().contains("canceling statement due to")
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
            "-H",
            "Content-Type: application/json",
            "-d",
            &body,
            &format!("{api}/proxies/{proxy}/toxics"),
        ])
        .status()
        .context("curl toxiproxy add latency")?;
    if !status.success() {
        bail!("failed to add toxiproxy latency toxic");
    }
    Ok(())
}

/// `pg_cancel_backend` during a managed-table read must abort cleanly and leave
/// the relation readable afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_managed_select_while_reading_recovers() -> Result<()> {
    common::require_pgrx_server().await?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "query_cancel").await?;
        let table = db
            .create_indexed_items_table("query_cancel_items", 64)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        db.flush_table(&table.relation).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 64).await?;

        let reader = common::connect_peer(&db).await?;
        let cancel_token = reader.cancel_token();
        let reader_pid: i32 = reader
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);

        let relation = table.relation.clone();
        let read_handle = tokio::spawn(async move {
            // Per-row sleep keeps the merge scan in flight long enough to cancel.
            reader
                .simple_query(&format!(
                    "SELECT count(*) FROM {relation} WHERE pg_sleep(0.02) IS NOT NULL"
                ))
                .await
        });

        let mut saw_active = false;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let active: bool = db
                .client
                .query_one(
                    r#"
                    SELECT EXISTS (
                      SELECT 1
                      FROM pg_stat_activity
                      WHERE pid = $1
                        AND state = 'active'
                        AND query ILIKE '%pg_sleep%'
                    )
                    "#,
                    &[&reader_pid],
                )
                .await?
                .get(0);
            if active {
                saw_active = true;
                break;
            }
            if read_handle.is_finished() {
                break;
            }
        }
        anyhow::ensure!(
            saw_active || !read_handle.is_finished(),
            "managed SELECT finished before cancel could be issued"
        );

        cancel_token
            .cancel_query(tokio_postgres::NoTls)
            .await
            .context("cancel_query while merge scan reading")?;

        let read_result = read_handle.await.context("join canceled reader task")?;
        match read_result {
            Ok(_) => bail!("expected canceled SELECT to error, but it succeeded"),
            Err(error) => anyhow::ensure!(
                is_query_canceled(&error),
                "expected query canceled, got: {error}"
            ),
        }

        // Follow-up read on a fresh session must succeed after cancel.
        let probe = common::connect_peer(&db).await?;
        let count: i64 = probe
            .query_one(
                &format!("SELECT count(*)::bigint FROM {}", table.relation),
                &[],
            )
            .await
            .context("post-cancel SELECT")?
            .get(0);
        assert_eq!(count, 64);
    }

    Ok(())
}

/// Toxiproxy latency on MinIO GETs: cold SELECT times out / cancels without
/// corrupting catalog or stranding the relation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn toxiproxy_latency_cancels_cold_read_without_corrupt_catalog() -> Result<()> {
    if !toxiproxy_enabled() {
        common::log_always(
            "skipping toxiproxy cold-read cancel (set KOLDSTORE_TOXIPROXY=1 via scripts/ci/start-toxiproxy.sh)",
        );
        return Ok(());
    }
    toxiproxy_reset()?;
    toxiproxy_add_latency_ms(60_000)?;

    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start_minio(target, "toxi_read_cancel").await?;
        let table = db.create_indexed_items_table("toxi_read_items", 24).await?;
        db.manage_shared(&table.relation, "id").await?;
        // Flush before latency toxic would also hang; reset temporarily.
        toxiproxy_reset()?;
        db.flush_table(&table.relation).await?;
        common::assert_flush_pruned_hot_storage(&db.client, &table.relation, 24).await?;
        toxiproxy_add_latency_ms(60_000)?;

        let reader = common::connect_peer(&db).await?;
        reader
            .batch_execute("SET statement_timeout = '3s';")
            .await
            .context("SET statement_timeout")?;

        let select_result = reader
            .query_one(
                &format!("SELECT count(*)::bigint FROM {}", table.relation),
                &[],
            )
            .await;
        let _ = reader.batch_execute("RESET statement_timeout;").await;

        match select_result {
            Ok(rows) => bail!(
                "latency toxic must prevent cold SELECT completion, got count={}",
                rows.get::<_, i64>(0)
            ),
            Err(error) => {
                let message = error.to_string();
                anyhow::ensure!(
                    is_query_canceled(&error)
                        || message.contains("timeout")
                        || message.contains("canceling statement"),
                    "expected timeout/cancel under toxiproxy latency, got: {message}"
                );
            }
        }

        toxiproxy_reset()?;
        let recovered: i64 = db
            .client
            .query_one(
                &format!("SELECT count(*)::bigint FROM {}", table.relation),
                &[],
            )
            .await
            .context("SELECT after toxiproxy reset")?
            .get(0);
        assert_eq!(recovered, 24);
        assert_eq!(
            common::published_manifest_count(&db.client, &table.relation).await?,
            1
        );
    }

    toxiproxy_reset()?;
    Ok(())
}
