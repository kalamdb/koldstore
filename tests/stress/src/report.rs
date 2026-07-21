//! JSON + text report writer under `target/stress`.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::config::StressConfig;
use crate::metrics::MetricsSnapshot;
use crate::support::log_always;
use crate::watchdog::WatchdogPeaks;

/// Full soak report artifact.
#[derive(Debug, Serialize)]
pub struct StressReport {
    pub packs: Vec<&'static str>,
    pub mirror_mode: &'static str,
    pub soak_secs: u64,
    pub baseline: MetricsSnapshot,
    pub soak: MetricsSnapshot,
    pub watchdog: WatchdogPeaks,
    pub cold_segments_messages: i64,
    pub seed_rows: i64,
    pub passed: bool,
    pub notes: Vec<String>,
}

/// Writes `report.json` and `report.txt` under `target/stress/<run_id>/`.
///
/// # Errors
///
/// Returns an error when filesystem writes fail.
pub fn write_report(report: &StressReport, run_id: &str) -> Result<PathBuf> {
    let dir = report_dir(run_id)?;
    let json_path = dir.join("report.json");
    let text_path = dir.join("report.txt");
    let json = serde_json::to_string_pretty(report).context("serialize stress report")?;
    fs::write(&json_path, json).with_context(|| format!("write {}", json_path.display()))?;
    fs::write(&text_path, format_text(report))
        .with_context(|| format!("write {}", text_path.display()))?;
    log_always(format!("report written to {}", dir.display()));
    Ok(dir)
}

fn report_dir(run_id: &str) -> Result<PathBuf> {
    let root = std::env::var("KOLDSTORE_STRESS_REPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/stress"));
    let dir = root.join(run_id);
    fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    Ok(dir)
}

/// Writes report then mirrors into `target/stress/latest/`.
///
/// # Errors
///
/// Returns an error when filesystem writes fail.
pub fn write_report_with_latest(report: &StressReport, run_id: &str) -> Result<PathBuf> {
    let dir = write_report(report, run_id)?;
    if run_id != "latest" {
        let _ = write_report(report, "latest");
    }
    Ok(dir)
}

fn format_text(report: &StressReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "chat-penetration packs={:?} mirror={} soak={}s passed={}\n",
        report.packs, report.mirror_mode, report.soak_secs, report.passed
    ));
    out.push_str(&format!(
        "seed_rows={} cold_segments_messages={}\n",
        report.seed_rows, report.cold_segments_messages
    ));
    out.push_str(&format!(
        "watchdog peak_fds={} peak_conns={} samples={} fd_unsupported={}\n",
        report.watchdog.peak_open_fds,
        report.watchdog.peak_connections,
        report.watchdog.samples,
        report.watchdog.fd_unsupported
    ));
    out.push_str("baseline ops:\n");
    for (op, pct) in &report.baseline.ops {
        out.push_str(&format!("  {op}: n={} p95={}us\n", pct.count, pct.p95_us));
    }
    out.push_str("soak ops:\n");
    for (op, pct) in &report.soak.ops {
        out.push_str(&format!("  {op}: n={} p95={}us\n", pct.count, pct.p95_us));
    }
    out.push_str(&format!(
        "counters inserts={} updates={} history={} joins={} cold_upd={} cold_del={} flushes={}\n",
        report.soak.inserts,
        report.soak.updates,
        report.soak.history_selects,
        report.soak.join_selects,
        report.soak.cold_updates,
        report.soak.cold_deletes,
        report.soak.flushes
    ));
    for note in &report.notes {
        out.push_str(&format!("note: {note}\n"));
    }
    out
}

/// Summarizes config for the log header.
pub fn log_config(config: &StressConfig) {
    log_always(format!(
        "config packs={:?} mirror={} soak={:?} writers={} history={} tenants={} conv/tenant={} \
         payload={}B blob={}B multiplier={}",
        config.packs.names(),
        config.mirror_mode.as_str(),
        config.soak,
        config.clients,
        config.history_clients,
        config.tenants,
        config.conversations_per_tenant,
        config.payload_bytes,
        config.bytea_bytes,
        config.latency_multiplier
    ));
    log_always(format!(
        "flush policy hot_row_limit={} min_flush_rows={} max_rows_per_file={} writer_delay={:?}",
        config.hot_row_limit, config.min_flush_rows, config.max_rows_per_file, config.writer_delay
    ));
}

/// Ensures a parent path exists (used by tests).
pub fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}
