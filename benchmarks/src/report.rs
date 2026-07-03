//! Benchmark report schema.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Complete benchmark report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkReport {
    /// Suite name.
    pub suite: String,
    /// Creation timestamp.
    pub generated_at: DateTime<Utc>,
    /// Machine metadata.
    pub machine: MachineMetadata,
    /// Test results.
    pub results: Vec<BenchmarkResult>,
}

impl BenchmarkReport {
    /// Creates an empty report for scaffolding and CI smoke checks.
    #[must_use]
    pub fn empty(suite: impl Into<String>) -> Self {
        Self {
            suite: suite.into(),
            generated_at: Utc::now(),
            machine: MachineMetadata::default(),
            results: Vec::new(),
        }
    }
}

/// Machine and environment metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineMetadata {
    /// PostgreSQL version under test.
    pub postgres_version: Option<String>,
    /// Object-store backend under test.
    pub object_store_backend: Option<String>,
    /// Host OS.
    pub os: Option<String>,
    /// CPU model or CI runner class.
    pub cpu: Option<String>,
    /// Total memory in bytes.
    pub memory_bytes: Option<u64>,
}

/// One benchmark result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Benchmark name.
    pub name: String,
    /// Rows tested.
    pub row_count: u64,
    /// Throughput in operations per second.
    pub throughput_ops_sec: f64,
    /// p50 latency in milliseconds.
    pub p50_ms: f64,
    /// p95 latency in milliseconds.
    pub p95_ms: f64,
    /// p99 latency in milliseconds.
    pub p99_ms: f64,
    /// Peak resident set size in bytes.
    pub peak_rss_bytes: Option<u64>,
    /// Allocated bytes observed during the scenario.
    pub allocated_bytes: Option<u64>,
    /// Verdict.
    pub passed: bool,
}

/// Renders a compact HTML summary for CI artifacts.
#[must_use]
pub fn to_html_summary(report: &BenchmarkReport) -> String {
    let rows = report
        .results
        .iter()
        .map(|result| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{:.2}</td><td>{:.2}</td><td>{:.2}</td><td>{:.2}</td><td>{}</td></tr>",
                result.name,
                result.row_count,
                result.throughput_ops_sec,
                result.p50_ms,
                result.p95_ms,
                result.p99_ms,
                result.passed
            )
        })
        .collect::<Vec<_>>()
        .join("");

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{}</title></head><body><h1>{}</h1><table><thead><tr><th>scenario</th><th>rows</th><th>ops/s</th><th>p50</th><th>p95</th><th>p99</th><th>passed</th></tr></thead><tbody>{}</tbody></table></body></html>",
        report.suite, report.suite, rows
    )
}
