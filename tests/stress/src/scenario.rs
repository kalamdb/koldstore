//! End-to-end chat penetration scenario: seed → baseline → soak → assert/report.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::baseline::{assert_latency_gates, run_baseline};
use crate::config::StressConfig;
use crate::control::SoakControl;
use crate::e2e;
use crate::metrics::Metrics;
use crate::report::{log_config, write_report_with_latest, StressReport};
use crate::schema::create_and_manage;
use crate::support::log_always;
use crate::watchdog::{spawn_watchdog, Watchdog};
use crate::workload::{assert_post_soak, seed_history, spawn_soak_workers, IdSeq};

/// Project-local cold storage root: `<repo>/tmp/chat_penetration`.
fn project_storage_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tmp/chat_penetration")
}

/// Clears and recreates `tmp/chat_penetration`, then points the e2e fixture at it.
///
/// # Errors
///
/// Returns an error when the directory cannot be reset.
fn prepare_project_storage() -> Result<PathBuf> {
    let root = project_storage_root();
    if root.exists() {
        std::fs::remove_dir_all(&root).with_context(|| format!("clear {}", root.display()))?;
    }
    std::fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
    let abs = root
        .canonicalize()
        .with_context(|| format!("canonicalize {}", root.display()))?;
    // SAFETY: single-threaded before workers spawn; intentional for this harness.
    unsafe {
        std::env::set_var("KOLDSTORE_E2E_STORAGE_ROOT", &abs);
        std::env::set_var("KOLDSTORE_E2E_KEEP_STORAGE", "1");
    }
    log_always(format!("cold storage root {}", abs.display()));
    Ok(abs)
}

/// Runs the full penetration scenario against a live pgrx cluster.
///
/// # Errors
///
/// Returns an error when setup, soak workers, latency gates, or spot-checks fail.
pub async fn run_chat_penetration() -> Result<()> {
    e2e::require_pgrx_server().await?;
    let config = StressConfig::from_env()?;
    log_config(&config);
    let storage_root = prepare_project_storage()?;

    let target = e2e::local_pg_matrix()
        .into_iter()
        .next()
        .context("no local pg target configured")?;
    let db = e2e::TestDb::start(target, "chat_penetration").await?;
    log_always(format!(
        "fixture storage_root={} (under {})",
        db.storage_root.display(),
        storage_root.display()
    ));

    let schema = create_and_manage(&db.client, &db.schema, &db.storage_name, &config).await?;
    let ids = Arc::new(IdSeq::new(1));
    let seeded = seed_history(&db.client, &schema, &config, &ids).await?;
    let seed_max_id = seeded; // message ids were 1..=seeded before sibling bump
    log_always(format!(
        "seed complete: {seeded} messages; next_id={}",
        ids.current()
    ));

    let segments = e2e::cold_segment_count(&db.client, &schema.messages).await?;
    anyhow::ensure!(
        segments > 0,
        "expected cold segments after seed flush, got {segments}"
    );

    let metrics = Arc::new(Metrics::new());
    let mut next_id = ids.current();
    let baseline = run_baseline(
        &db.client,
        &schema,
        &config,
        &metrics,
        &mut next_id,
        seed_max_id,
    )
    .await?;
    // Align shared allocator with baseline inserts.
    ids.set(next_id);
    metrics.clear_latency_samples();

    let control = SoakControl::new();
    let watchdog = Arc::new(Watchdog::new());
    let watchdog_handle = spawn_watchdog(
        db.target.clone(),
        Arc::clone(&control),
        Arc::clone(&watchdog),
        config.max_open_fds,
        config.max_connections,
    );

    log_always(format!(
        "soak start for {:?} (progress every {:?})",
        config.soak, config.progress_interval
    ));
    let workers = spawn_soak_workers(
        db.target.clone(),
        schema.clone(),
        config.clone(),
        Arc::clone(&ids),
        Arc::clone(&metrics),
        Arc::clone(&control),
        seed_max_id,
    );

    let soak_deadline = tokio::time::Instant::now() + config.soak;
    let mut ticker = tokio::time::interval(config.progress_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // First tick fires immediately; print after the first full interval.
    ticker.tick().await;
    let soak_started = std::time::Instant::now();
    loop {
        if control.is_fatal() {
            break;
        }
        tokio::select! {
            _ = ticker.tick() => {
                let snap = metrics.snapshot();
                log_always(snap.progress_line(soak_started.elapsed(), config.soak));
            }
            _ = tokio::time::sleep_until(soak_deadline) => {
                break;
            }
        }
        if soak_started.elapsed() >= config.soak {
            break;
        }
    }
    // Final in-soak snapshot before stop.
    log_always(
        metrics
            .snapshot()
            .progress_line(soak_started.elapsed(), config.soak),
    );
    control.request_stop();
    log_always("soak stop signaled; joining workers");

    for handle in workers {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                metrics.worker_errors.fetch_add(1, Ordering::Relaxed);
                control.note_db_error("worker task", &err);
            }
            Err(err) => {
                metrics.worker_errors.fetch_add(1, Ordering::Relaxed);
                log_always(format!("worker join error: {err}"));
            }
        }
    }
    let _ = watchdog_handle.await;

    if let Some(reason) = control.take_fatal_reason() {
        anyhow::bail!("{reason}");
    }

    assert_post_soak(&db.client, &schema, &config).await?;
    let soak = metrics.snapshot();
    assert_latency_gates(
        &baseline,
        &soak,
        config.latency_multiplier,
        config.absolute_latency_floor_us,
    )?;

    let cold_segments = e2e::cold_segment_count(&db.client, &schema.messages).await?;
    let run_id = format!("{}", chrono::Utc::now().format("%Y%m%dT%H%M%SZ"));
    let report = StressReport {
        packs: config.packs.names(),
        mirror_mode: config.mirror_mode.as_str(),
        soak_secs: config.soak.as_secs(),
        baseline,
        soak,
        watchdog: watchdog.peaks(),
        cold_segments_messages: cold_segments,
        seed_rows: seeded,
        passed: true,
        notes: vec![format!(
            "worker_errors={}",
            metrics.worker_errors.load(Ordering::Relaxed)
        )],
    };
    write_report_with_latest(&report, &run_id)?;

    log_always("chat penetration passed");
    Ok(())
}
