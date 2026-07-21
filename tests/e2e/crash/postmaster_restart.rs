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

    let start = Command::new("cargo")
        .args(["pgrx", "start", &feature])
        .output()
        .context("cargo pgrx start after immediate stop")?;
    if !start.status.success() {
        bail!(
            "cargo pgrx start failed: {}",
            String::from_utf8_lossy(&start.stderr)
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
    common::require_pgrx_server().await?;

    let target = common::scenario_pg_matrix()
        .into_iter()
        .next()
        .context("no pgrx target")?;
    let version = target.version;
    let mode = common::selected_mirror_capture_mode()?.as_str();

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
              auto_flush => false,
              mirror_capture_mode => $3
            )
            "#,
            &[&relation, &db.storage_name, &mode],
        )
        .await?;

    // Hold the failpoint barrier so flush parks at wait:after_select_rows.
    db.client
        .execute("SELECT pg_advisory_lock($1)", &[&0x4B4F_4C44_i64])
        .await?;

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
    drop(db);

    let reopen = common::PgTarget {
        version,
        port,
        dbname,
    };
    let client = common::wait_for_postgres(&reopen).await?;
    client
        .batch_execute("SET koldstore.failpoint = '';")
        .await
        .context("clear failpoint after restart")?;
    // Best-effort unlock if the lock survived (usually does not after immediate stop).
    let _ = client.execute("SELECT pg_advisory_unlock_all()", &[]).await;

    let flushed = client
        .query_one(
            "SELECT koldstore.flush_table($1::text::regclass)",
            &[&relation],
        )
        .await
        .context("retry flush after postmaster restart")?;
    let _rows: i64 = flushed.get(0);

    common::assert_pk_unique(&client, &relation, &["id"]).await?;
    let visible = common::relation_row_count(&client, &relation).await?;
    assert_eq!(
        visible, 36,
        "expected 36 visible rows after postmaster restart recovery, got {visible}"
    );
    Ok(())
}
