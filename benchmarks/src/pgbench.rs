//! PostgreSQL pgbench integration.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use tokio::process::Command;

/// One pgbench workload script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgbenchWorkload {
    /// Stable scenario name.
    pub name: String,
    /// SQL script body.
    pub script: String,
}

/// pgbench run configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgbenchConfig {
    /// PostgreSQL connection string.
    pub database_url: String,
    /// Number of concurrent clients.
    pub clients: usize,
    /// Number of pgbench worker threads.
    pub jobs: usize,
    /// Duration in seconds.
    pub seconds: u64,
}

/// Parsed pgbench measurements.
#[derive(Debug, Clone, PartialEq)]
pub struct PgbenchMeasurement {
    /// Scenario name.
    pub name: String,
    /// Completed transaction count.
    pub transactions: u64,
    /// Throughput in transactions per second.
    pub throughput_ops_sec: f64,
    /// p50 transaction latency in milliseconds.
    pub p50_ms: f64,
    /// p95 transaction latency in milliseconds.
    pub p95_ms: f64,
    /// p99 transaction latency in milliseconds.
    pub p99_ms: f64,
}

/// Runs pgbench for one workload and parses per-transaction latency logs.
///
/// # Errors
///
/// Returns an error when pgbench fails or its log files cannot be parsed.
pub async fn run_pgbench(
    config: &PgbenchConfig,
    workload: &PgbenchWorkload,
    workspace: &Path,
) -> Result<PgbenchMeasurement> {
    let script_path = workspace.join(format!("{}.sql", workload.name));
    let log_prefix = workspace.join(format!("{}.log", workload.name));
    fs::write(&script_path, &workload.script)
        .with_context(|| format!("write pgbench script {}", script_path.display()))?;

    let output = Command::new("pgbench")
        .arg("-n")
        .arg("-M")
        .arg("prepared")
        .arg("-c")
        .arg(config.clients.to_string())
        .arg("-j")
        .arg(config.jobs.to_string())
        .arg("-T")
        .arg(config.seconds.to_string())
        .arg("-l")
        .arg("--log-prefix")
        .arg(&log_prefix)
        .arg("--random-seed")
        .arg("1")
        .arg("-f")
        .arg(&script_path)
        .arg(&config.database_url)
        .output()
        .await
        .context("run pgbench")?;

    if !output.status.success() {
        anyhow::bail!(
            "pgbench {} failed: {}",
            workload.name,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let latencies = read_latency_logs(&log_prefix)?;
    let transactions = latencies.len() as u64;
    let throughput_ops_sec = transactions as f64 / config.seconds as f64;
    let (p50_ms, p95_ms, p99_ms) = percentiles(latencies);

    Ok(PgbenchMeasurement {
        name: workload.name.clone(),
        transactions,
        throughput_ops_sec,
        p50_ms,
        p95_ms,
        p99_ms,
    })
}

fn read_latency_logs(log_prefix: &Path) -> Result<Vec<f64>> {
    let directory = log_prefix.parent().unwrap_or_else(|| Path::new("."));
    let prefix = log_prefix
        .file_name()
        .and_then(|name| name.to_str())
        .context("pgbench log prefix must be valid UTF-8")?;
    let mut latencies = Vec::new();

    for entry in fs::read_dir(directory).context("read pgbench log directory")? {
        let path = entry.context("read pgbench log entry")?.path();
        if !is_pgbench_log_file(&path, prefix) {
            continue;
        }
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("read pgbench log {}", path.display()))?;
        for line in contents.lines() {
            if let Some(latency_ms) = parse_latency_ms(line) {
                latencies.push(latency_ms);
            }
        }
    }

    anyhow::ensure!(
        !latencies.is_empty(),
        "pgbench produced no transaction logs"
    );
    Ok(latencies)
}

fn is_pgbench_log_file(path: &Path, prefix: &str) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(prefix))
}

fn parse_latency_ms(line: &str) -> Option<f64> {
    let micros = line.split_whitespace().nth(2)?.parse::<f64>().ok()?;
    Some(micros / 1000.0)
}

fn percentiles(mut latencies: Vec<f64>) -> (f64, f64, f64) {
    latencies.sort_by(f64::total_cmp);
    (
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
    )
}

fn percentile(sorted: &[f64], percentile: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index]
}
