//! Latency sample accumulation and percentile helpers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use serde::Serialize;

/// Named operation keys used in baseline and soak reports.
pub const OP_INSERT: &str = "insert";
pub const OP_HISTORY: &str = "history_select";
pub const OP_COLD_UPDATE: &str = "cold_update";
pub const OP_JOIN: &str = "join_select";

/// Thread-safe latency + counter sink shared across workers.
#[derive(Debug, Default)]
pub struct Metrics {
    series: Mutex<HashMap<String, Vec<u64>>>,
    pub inserts: AtomicU64,
    pub updates: AtomicU64,
    pub deletes: AtomicU64,
    pub history_selects: AtomicU64,
    pub join_selects: AtomicU64,
    pub cold_updates: AtomicU64,
    pub cold_deletes: AtomicU64,
    pub flushes: AtomicU64,
    pub flush_errors: AtomicU64,
    pub worker_errors: AtomicU64,
}

impl Metrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, op: &str, elapsed_us: u64) {
        let mut guard = self.series.lock().expect("metrics lock");
        guard.entry(op.to_string()).or_default().push(elapsed_us);
    }

    pub fn time<T>(&self, op: &str, f: impl FnOnce() -> T) -> T {
        let started = Instant::now();
        let out = f();
        self.record(op, started.elapsed().as_micros() as u64);
        out
    }

    pub async fn time_async<T, E>(
        &self,
        op: &str,
        fut: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let started = Instant::now();
        let out = fut.await;
        self.record(op, started.elapsed().as_micros() as u64);
        out
    }

    /// Snapshot percentiles and counters for reporting / gating.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        let series = self.series.lock().expect("metrics lock");
        let mut ops = HashMap::new();
        for (name, samples) in series.iter() {
            ops.insert(name.clone(), Percentiles::from_samples(samples));
        }
        MetricsSnapshot {
            ops,
            inserts: self.inserts.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            history_selects: self.history_selects.load(Ordering::Relaxed),
            join_selects: self.join_selects.load(Ordering::Relaxed),
            cold_updates: self.cold_updates.load(Ordering::Relaxed),
            cold_deletes: self.cold_deletes.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
            flush_errors: self.flush_errors.load(Ordering::Relaxed),
            worker_errors: self.worker_errors.load(Ordering::Relaxed),
        }
    }

    /// Clears latency samples (counters kept). Used between baseline and soak.
    pub fn clear_latency_samples(&self) {
        self.series.lock().expect("metrics lock").clear();
    }
}

/// Sorted-sample percentiles in microseconds.
#[derive(Debug, Clone, Serialize)]
pub struct Percentiles {
    pub count: usize,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

impl Percentiles {
    #[must_use]
    pub fn from_samples(samples: &[u64]) -> Self {
        if samples.is_empty() {
            return Self {
                count: 0,
                p50_us: 0,
                p95_us: 0,
                p99_us: 0,
                max_us: 0,
            };
        }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let pct = |p: f64| {
            let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
            sorted[idx.min(sorted.len() - 1)]
        };
        Self {
            count: sorted.len(),
            p50_us: pct(0.50),
            p95_us: pct(0.95),
            p99_us: pct(0.99),
            max_us: *sorted.last().unwrap_or(&0),
        }
    }
}

/// Serializable metrics view.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub ops: HashMap<String, Percentiles>,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub history_selects: u64,
    pub join_selects: u64,
    pub cold_updates: u64,
    pub cold_deletes: u64,
    pub flushes: u64,
    pub flush_errors: u64,
    pub worker_errors: u64,
}

impl MetricsSnapshot {
    #[must_use]
    pub fn p95(&self, op: &str) -> Option<u64> {
        self.ops.get(op).filter(|p| p.count > 0).map(|p| p.p95_us)
    }

    /// One-line soak progress: counters + live p50/p95/p99 for key ops.
    #[must_use]
    pub fn progress_line(&self, elapsed: std::time::Duration, soak: std::time::Duration) -> String {
        let fmt_op = |op: &str| -> String {
            match self.ops.get(op) {
                Some(p) if p.count > 0 => format!(
                    "{op}: n={} p50={}us p95={}us p99={}us",
                    p.count, p.p50_us, p.p95_us, p.p99_us
                ),
                _ => format!("{op}: (no samples yet)"),
            }
        };
        format!(
            "soak {elapsed:.0?}/{soak:.0?} messages={} history={} joins={} \
             cold_upd={} cold_del={} flushes={} errors={} | {} | {} | {} | {}",
            self.inserts,
            self.history_selects,
            self.join_selects,
            self.cold_updates,
            self.cold_deletes,
            self.flushes,
            self.worker_errors + self.flush_errors,
            fmt_op(OP_INSERT),
            fmt_op(OP_HISTORY),
            fmt_op(OP_JOIN),
            fmt_op(OP_COLD_UPDATE),
        )
    }
}
