//! Real postmaster restart mid-flush (`pg_ctl -m immediate`), then recover/retry.
//!
//! Gated by `KOLDSTORE_CRASH_POSTMASTER_RESTART=1` because it stops the shared
//! pgrx cluster. Nightly crash readiness enables the gate with `--test-threads 1`.

use crate::common;

use anyhow::{bail, Context, Result};
use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;

fn postmaster_restart_enabled() -> bool {
    matches!(
        std::env::var("KOLDSTORE_CRASH_POSTMASTER_RESTART")
            .ok()
            .as_deref(),
        Some("1") | Some("true")
    )
}

fn pgrx_data_dir(version: u16) -> String {
    let home = std::env::var("PGRX_HOME").unwrap_or_else(|_| {
        let user_home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        format!("{user_home}/.pgrx")
    });
    format!("{home}/data-{version}")
}

fn pgrx_log_file(version: u16) -> String {
    let home = std::env::var("PGRX_HOME").unwrap_or_else(|_| {
        let user_home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        format!("{user_home}/.pgrx")
    });
    format!("{home}/{version}.log")
}

fn pg_ctl_bin(version: u16) -> Result<String> {
    if let Ok(pg_config) = std::env::var("PGRX_PG_CONFIG") {
        let bin = std::path::Path::new(&pg_config)
            .parent()
            .map(|p| p.join("pg_ctl"))
            .context("pg_config parent")?;
        return Ok(bin.to_string_lossy().into_owned());
    }
    let output = Command::new("cargo")
        .args(["pgrx", "info", "pg-config", &version.to_string()])
        .output()
        .context("cargo pgrx info pg-config")?;
    if !output.status.success() {
        bail!(
            "cargo pgrx info pg-config failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let pg_config = String::from_utf8(output.stdout)?.trim().to_string();
    let bin = std::path::Path::new(&pg_config)
        .parent()
        .map(|p| p.join("pg_ctl"))
        .context("pg_config parent")?;
    Ok(bin.to_string_lossy().into_owned())
}

/// Same runtime conf as `scripts/run-pg-e2e.sh` — `cargo pgrx start` applies these
/// only via `-c` flags (they are not persisted in postgresql.conf).
fn pgrx_start_conf_args() -> Vec<String> {
    let workers = std::env::var("KOLDSTORE_E2E_THREADS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .map(|threads| threads.saturating_add(8).max(16))
        .unwrap_or(16);
    vec![
        "--postgresql-conf".into(),
        "wal_level=logical".into(),
        "--postgresql-conf".into(),
        "shared_preload_libraries=koldstore".into(),
        "--postgresql-conf".into(),
        format!("max_worker_processes={workers}"),
        "--postgresql-conf".into(),
        format!("max_replication_slots={workers}"),
        "--postgresql-conf".into(),
        format!("max_wal_senders={workers}"),
    ]
}

fn immediate_stop_and_start(version: u16) -> Result<()> {
    let data_dir = pgrx_data_dir(version);
    let pg_ctl = pg_ctl_bin(version)?;
    let feature = format!("pg{version}");

    let stop = Command::new(&pg_ctl)
        .args(["-D", &data_dir, "-m", "immediate", "stop", "-w", "-t", "15"])
        .output()
        .with_context(|| format!("pg_ctl immediate stop via {pg_ctl}"))?;
    if !stop.status.success() {
        let _ = Command::new("cargo")
            .args(["pgrx", "stop", &feature])
            .status();
    }

    let mut start = Command::new("cargo");
    start.arg("pgrx").arg("start").arg(&feature);
    for arg in pgrx_start_conf_args() {
        start.arg(arg);
    }
    let start = start
        .output()
        .context("cargo pgrx start after immediate stop")?;
    if !start.status.success() {
        let log_path = pgrx_log_file(version);
        let log_tail = std::fs::read_to_string(&log_path)
            .map(|contents| {
                contents
                    .lines()
                    .rev()
                    .take(80)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_else(|_| format!("(could not read {log_path})"));
        bail!(
            "cargo pgrx start failed: {}\n──── {} ────\n{log_tail}",
            String::from_utf8_lossy(&start.stderr),
            log_path
        );
    }
    Ok(())
}

#[tokio::test]
async fn postmaster_immediate_restart_mid_flush_recovers() -> Result<()> {
    if !postmaster_restart_enabled() {
        common::log_always(
            "skipping postmaster restart crash test (set KOLDSTORE_CRASH_POSTMASTER_RESTART=1)",
        );
        return Ok(());
    }
    // Immediate restart kills every backend on the shared pgrx cluster.
    let _cluster = common::acquire_cluster_exclusive()?;
    common::require_pgrx_server().await?;

    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("no pgrx target")?;
    let version = target.version;

    let db = common::TestDb::start(target.clone(), "crash_pm").await?;
    let dbname = db.target.dbname.clone();
    let port = db.target.port;
    let table = db.create_indexed_items_table("pm_items", 36).await?;
    let relation = table.relation.clone();

    db.client
        .batch_execute("SET koldstore.min_max_rows_per_file = 1;")
        .await?;
    db.client
        .execute(
            r#"
            SELECT koldstore.manage_table(
              table_name => $1::text::regclass,
              storage => $2,
              hot_row_limit => 6,
              min_flush_rows => 1,
              max_rows_per_file => 12,
              migration_order_by => 'id',
              auto_flush => false
            )
            "#,
            &[&relation, &db.storage_name],
        )
        .await?;

    // Hold the per-database failpoint barrier so flush parks at wait:after_select_rows.
    common::barrier_lock(&db.client).await?;

    let flush_target = common::PgTarget {
        version,
        port,
        dbname: dbname.clone(),
    };
    let flush_peer = common::connect(&flush_target).await?;
    let flush_relation = relation.clone();
    let flush_handle = tokio::spawn(async move {
        let _ = flush_peer
            .batch_execute("SET koldstore.failpoint = 'wait:after_select_rows';")
            .await;
        let _ = flush_peer
            .query_one(
                "SELECT koldstore.flush_table($1::text::regclass)::text",
                &[&flush_relation],
            )
            .await;
    });

    sleep(Duration::from_millis(750)).await;
    immediate_stop_and_start(version)?;
    let _ = flush_handle.await;
    // Keep `db` alive until the end: TestDb::drop deletes the filesystem cold
    // storage root, which recover_segments / retry flush still need.

    let reopen = common::PgTarget {
        version,
        port,
        dbname,
    };
    let client = common::wait_for_postgres(&reopen).await?;
    let wal_level: String = client
        .query_one("SHOW wal_level", &[])
        .await
        .context("SHOW wal_level after restart")?
        .get(0);
    anyhow::ensure!(
        wal_level == "logical",
        "post-restart wal_level must be logical (got {wal_level}); cargo pgrx start must pass --postgresql-conf"
    );
    let preload: String = client
        .query_one("SHOW shared_preload_libraries", &[])
        .await
        .context("SHOW shared_preload_libraries after restart")?
        .get(0);
    anyhow::ensure!(
        preload
            .split(',')
            .map(str::trim)
            .any(|entry| entry == "koldstore"),
        "post-restart shared_preload_libraries must include koldstore (got {preload})"
    );
    client
        .batch_execute("SET koldstore.failpoint = '';")
        .await
        .context("clear failpoint after restart")?;
    // Best-effort unlock if the lock survived (usually does not after immediate stop).
    let _ = client.execute("SELECT pg_advisory_unlock_all()", &[]).await;

    // Same recover → retry path as failpoint crash recovery.
    let _ = client
        .query_one(
            "SELECT koldstore.recover_segments($1::text::regclass, false)",
            &[&relation],
        )
        .await
        .context("recover_segments after postmaster restart")?;

    // flush_table returns uuid; cast to text like other E2E flush callers.
    let flushed = client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass)::text",
            &[&relation],
        )
        .await
        .context("retry flush after postmaster restart")?;
    let job_id: String = flushed.get(0);
    let rows_flushed: i64 = client
        .query_one(
            "SELECT rows_flushed FROM koldstore.jobs WHERE id = $1::text::uuid",
            &[&job_id],
        )
        .await
        .with_context(|| format!("load rows_flushed for flush job {job_id}"))?
        .get(0);
    common::log_always(format!(
        "postmaster restart: retry flushed rows_flushed={rows_flushed}"
    ));

    common::fence_async_mirror(&client).await?;
    common::assert_pk_unique(&client, &relation, &["id"]).await?;
    let visible = common::relation_row_count(&client, &relation).await?;
    assert_eq!(
        visible, 36,
        "expected 36 visible rows after postmaster restart recovery, got {visible}"
    );

    // Policy retry may leave hot_row_limit rows hot; still require cold objects,
    // mirror/hot agreement, integrity, and on-disk parquet/manifest.
    crate::crash::invariants::assert_recovered_flush_data_plane(
        &client,
        &relation,
        &db.storage_root,
        crate::crash::invariants::RecoveredFlushExpect {
            visible_rows: 36,
            expect_hot_fully_pruned: false,
            min_cold_segments: 1,
            reference: None,
        },
    )
    .await
    .context("postmaster restart data-plane checks")?;
    Ok(())
}
