//! Live memory snapshots for E2E leak gates.
//!
//! Combines session `pg_backend_memory_contexts` totals with process RSS for
//! the SQL backend and matching PostgreSQL worker processes.

use anyhow::{Context, Result};
use koldstore_memory::{
    matched_processes_rss_bytes, process_rss_bytes, GrowthBudget, MemorySnapshot, PeakAllocation,
};
use tokio_postgres::Client;

/// Maximum retained context overhead for comparable plain vs koldstore workloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverheadBudget {
    /// Max extra retained context bytes for DML (`kold − plain`).
    pub max_dml_context_delta_overhead_bytes: u64,
    /// Max extra retained context bytes for hot-only queries.
    pub max_hot_query_context_delta_overhead_bytes: u64,
    /// Max absolute context-after gap for DML.
    pub max_dml_context_after_overhead_bytes: u64,
    /// Max absolute context-after gap for hot-only queries.
    pub max_hot_query_context_after_overhead_bytes: u64,
}

impl Default for OverheadBudget {
    fn default() -> Self {
        Self {
            max_dml_context_delta_overhead_bytes: 8 * 1024 * 1024,
            max_hot_query_context_delta_overhead_bytes: 4 * 1024 * 1024,
            max_dml_context_after_overhead_bytes: 16 * 1024 * 1024,
            max_hot_query_context_after_overhead_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Absolute peak-spike budgets for flush / large-query windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpikeBudget {
    /// Max RSS growth above baseline during the operation.
    pub max_rss_spike_bytes: u64,
    /// Max pg_context growth above baseline during the operation.
    pub max_context_spike_bytes: u64,
    /// Max retained RSS growth after the operation cools down.
    pub max_rss_retained_bytes: u64,
    /// Max retained context growth after cooldown.
    pub max_context_retained_bytes: u64,
}

impl Default for SpikeBudget {
    fn default() -> Self {
        Self {
            // Flush encode + workers can temporarily hold Parquet buffers.
            max_rss_spike_bytes: 512 * 1024 * 1024,
            max_context_spike_bytes: 256 * 1024 * 1024,
            max_rss_retained_bytes: 128 * 1024 * 1024,
            max_context_retained_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Captures PostgreSQL memory-context totals and RSS for the current backend
/// plus workers whose command line contains the cluster port.
///
/// # Errors
///
/// Returns an error when SQL sampling or process RSS sampling fails.
pub async fn capture_snapshot(client: &Client, pg_port: u16) -> Result<MemorySnapshot> {
    let row = client
        .query_one(
            r#"
            SELECT
              pg_backend_pid()::int4,
              COALESCE((
                SELECT sum(total_bytes)::bigint
                FROM pg_backend_memory_contexts
              ), 0)::bigint
            "#,
            &[],
        )
        .await
        .context("sample pg_backend_memory_contexts")?;
    let backend_pid: i32 = row.get(0);
    let pg_context_bytes = u64::try_from(row.get::<_, i64>(1)).unwrap_or(0);

    let backend_rss = process_rss_bytes(backend_pid)
        .map_err(|error| anyhow::anyhow!("backend rss pid {backend_pid}: {error}"))?;
    // Include flush / async-mirror workers belonging to this pgrx cluster.
    let worker_rss = matched_processes_rss_bytes(&format!(":{pg_port}"))
        .or_else(|_| matched_processes_rss_bytes(&format!("port={pg_port}")))
        .unwrap_or(0);
    let rss_bytes = backend_rss.max(worker_rss);

    Ok(MemorySnapshot {
        rss_bytes,
        pg_context_bytes,
        allocator_bytes: None,
    })
}

/// Polls process RSS while `work` runs so mid-operation spikes are visible.
///
/// Samples every `poll_every` against `pg_port` workers (flush executors share
/// the cluster). The SQL session snapshot is taken before/after on `client`.
///
/// # Errors
///
/// Returns an error when snapshot capture or `work` fails.
pub async fn measure_peak_during<F, Fut>(
    client: &Client,
    pg_port: u16,
    poll_every: std::time::Duration,
    work: F,
) -> Result<PeakAllocation>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let before = capture_snapshot(client, pg_port).await?;
    let peak_rss = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(before.rss_bytes));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_poller = std::sync::Arc::clone(&stop);
    let peak_rss_poller = std::sync::Arc::clone(&peak_rss);
    let poller = tokio::spawn(async move {
        while !stop_poller.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok(rss) = matched_processes_rss_bytes(&format!(":{pg_port}"))
                .or_else(|_| matched_processes_rss_bytes(&format!("port={pg_port}")))
            {
                peak_rss_poller.fetch_max(rss, std::sync::atomic::Ordering::Relaxed);
            }
            tokio::time::sleep(poll_every).await;
        }
    });

    let work_result = work().await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = poller.await;
    work_result?;

    let after = capture_snapshot(client, pg_port).await?;
    // Cool-down: catch delayed release after flush executor exit.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let cool = capture_snapshot(client, pg_port).await?;
    let polled = MemorySnapshot {
        rss_bytes: peak_rss.load(std::sync::atomic::Ordering::Relaxed),
        pg_context_bytes: before
            .pg_context_bytes
            .max(after.pg_context_bytes)
            .max(cool.pg_context_bytes),
        allocator_bytes: None,
    };
    PeakAllocation::from_samples(&[before, polled, after, cool])
        .context("peak measurement needs samples")
}

/// Loads growth budgets from environment overrides when present.
#[must_use]
pub fn growth_budget_from_env() -> GrowthBudget {
    let mut budget = GrowthBudget::default();
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_CONTEXT_GROWTH_BYTES") {
        budget.max_pg_context_growth_bytes = value;
    }
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_RSS_GROWTH_BYTES") {
        budget.max_rss_growth_bytes = value;
    }
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_CONTEXT_BYTES_PER_CYCLE") {
        budget.max_pg_context_bytes_per_cycle = value;
    }
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_RSS_BYTES_PER_CYCLE") {
        budget.max_rss_bytes_per_cycle = value;
    }
    budget
}

/// Loads peak-spike budgets (flush / large query) from the environment when set.
#[must_use]
pub fn spike_budget_from_env() -> SpikeBudget {
    let mut budget = SpikeBudget::default();
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_FLUSH_RSS_SPIKE_BYTES") {
        budget.max_rss_spike_bytes = value;
    }
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_FLUSH_CONTEXT_SPIKE_BYTES") {
        budget.max_context_spike_bytes = value;
    }
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_FLUSH_RSS_RETAINED_BYTES") {
        budget.max_rss_retained_bytes = value;
    }
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_FLUSH_CONTEXT_RETAINED_BYTES") {
        budget.max_context_retained_bytes = value;
    }
    budget
}

/// Returns warmup cycle count (default 3).
#[must_use]
pub fn warmup_cycles() -> usize {
    env_usize("KOLDSTORE_MEMORY_WARMUP_CYCLES").unwrap_or(3)
}

/// Returns measured cycle count after warmup (default 12).
#[must_use]
pub fn measure_cycles() -> usize {
    env_usize("KOLDSTORE_MEMORY_MEASURE_CYCLES")
        .unwrap_or(12)
        .max(2)
}

/// Returns rows inserted per lifecycle cycle (default 128).
#[must_use]
pub fn batch_rows() -> i64 {
    env_i64("KOLDSTORE_MEMORY_BATCH_ROWS").unwrap_or(128).max(8)
}

/// Returns merge-scan SELECT repetitions per cycle (default 8).
#[must_use]
pub fn scan_reps() -> usize {
    env_usize("KOLDSTORE_MEMORY_SCAN_REPS").unwrap_or(8).max(2)
}

/// Row count for the large-query memory gate (default 20_000).
#[must_use]
pub fn large_query_rows() -> i64 {
    env_i64("KOLDSTORE_MEMORY_LARGE_QUERY_ROWS")
        .unwrap_or(20_000)
        .max(2_000)
}

/// Loads plain-vs-koldstore overhead budgets from the environment when set.
#[must_use]
pub fn overhead_budget_from_env() -> OverheadBudget {
    let mut budget = OverheadBudget::default();
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_DML_CONTEXT_DELTA_OVERHEAD_BYTES") {
        budget.max_dml_context_delta_overhead_bytes = value;
    }
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_HOT_QUERY_CONTEXT_DELTA_OVERHEAD_BYTES") {
        budget.max_hot_query_context_delta_overhead_bytes = value;
    }
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_DML_CONTEXT_AFTER_OVERHEAD_BYTES") {
        budget.max_dml_context_after_overhead_bytes = value;
    }
    if let Some(value) = env_u64("KOLDSTORE_MEMORY_MAX_HOT_QUERY_CONTEXT_AFTER_OVERHEAD_BYTES") {
        budget.max_hot_query_context_after_overhead_bytes = value;
    }
    budget
}

/// Measures before / mid / after snapshots around a two-half workload.
///
/// The mid snapshot is taken between `first_half` and `second_half` so spikes
/// during the operation are visible, not only retained growth at the end.
///
/// # Errors
///
/// Returns an error when snapshot capture or either workload half fails.
pub async fn measure_phase<F1, Fut1, F2, Fut2>(
    client: &Client,
    pg_port: u16,
    first_half: F1,
    second_half: F2,
) -> Result<PeakAllocation>
where
    F1: FnOnce() -> Fut1,
    Fut1: std::future::Future<Output = Result<()>>,
    F2: FnOnce() -> Fut2,
    Fut2: std::future::Future<Output = Result<()>>,
{
    let before = capture_snapshot(client, pg_port).await?;
    first_half().await?;
    let mid = capture_snapshot(client, pg_port).await?;
    second_half().await?;
    let after = capture_snapshot(client, pg_port).await?;
    // Cool-down sample: catch delayed context release / retention.
    let cool = capture_snapshot(client, pg_port).await?;
    PeakAllocation::from_samples(&[before, mid, after, cool])
        .context("phase measurement requires before/mid/after samples")
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

fn env_i64(name: &str) -> Option<i64> {
    std::env::var(name).ok()?.parse().ok()
}
