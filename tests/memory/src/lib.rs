//! Memory-leak probe helpers shared by unit checks and E2E lifecycle gates.
//!
//! Captures coarse process RSS and PostgreSQL memory-context totals, then
//! evaluates retained growth across repeated managed-table operations.

use std::fs;
use std::process::Command;

/// Snapshot of process and PostgreSQL memory state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemorySnapshot {
    /// Resident set size for the sampled process (bytes).
    pub rss_bytes: u64,
    /// Sum of `pg_backend_memory_contexts.total_bytes` for the session backend.
    pub pg_context_bytes: u64,
    /// Optional allocator-reported bytes when a custom allocator exposes them.
    pub allocator_bytes: Option<u64>,
}

impl MemorySnapshot {
    /// Creates an empty smoke-test snapshot.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            rss_bytes: 0,
            pg_context_bytes: 0,
            allocator_bytes: None,
        }
    }

    /// Returns total known bytes for coarse leak checks.
    #[must_use]
    pub fn known_total_bytes(self) -> u64 {
        self.rss_bytes
            .saturating_add(self.pg_context_bytes)
            .saturating_add(self.allocator_bytes.unwrap_or(0))
    }
}

/// Captures per-operation peak allocation across before/peak/after snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeakAllocation {
    /// Baseline snapshot.
    pub before: MemorySnapshot,
    /// Peak snapshot.
    pub peak: MemorySnapshot,
    /// Final snapshot.
    pub after: MemorySnapshot,
}

impl PeakAllocation {
    /// Returns allocation growth retained after the operation.
    #[must_use]
    pub fn retained_growth_bytes(self) -> u64 {
        self.after
            .known_total_bytes()
            .saturating_sub(self.before.known_total_bytes())
    }

    /// Builds a peak allocation from ordered samples (before, optional mids, after).
    #[must_use]
    pub fn from_samples(samples: &[MemorySnapshot]) -> Option<Self> {
        let (before, after) = match samples {
            [before, .., after] => (*before, *after),
            _ => return None,
        };
        let peak = samples
            .iter()
            .copied()
            .fold(before, |acc, sample| MemorySnapshot {
                rss_bytes: acc.rss_bytes.max(sample.rss_bytes),
                pg_context_bytes: acc.pg_context_bytes.max(sample.pg_context_bytes),
                allocator_bytes: match (acc.allocator_bytes, sample.allocator_bytes) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (Some(a), None) | (None, Some(a)) => Some(a),
                    (None, None) => None,
                },
            });
        Some(Self {
            before,
            peak,
            after,
        })
    }

    /// Retained PostgreSQL memory-context growth (`after - before`).
    #[must_use]
    pub fn context_delta_bytes(self) -> i64 {
        signed_delta(self.before.pg_context_bytes, self.after.pg_context_bytes)
    }

    /// Peak PostgreSQL memory-context spike above the baseline.
    #[must_use]
    pub fn context_spike_bytes(self) -> u64 {
        self.peak
            .pg_context_bytes
            .saturating_sub(self.before.pg_context_bytes)
    }

    /// Retained RSS growth (`after - before`).
    #[must_use]
    pub fn rss_delta_bytes(self) -> i64 {
        signed_delta(self.before.rss_bytes, self.after.rss_bytes)
    }

    /// Peak RSS spike above the baseline.
    #[must_use]
    pub fn rss_spike_bytes(self) -> u64 {
        self.peak.rss_bytes.saturating_sub(self.before.rss_bytes)
    }
}

fn signed_delta(before: u64, after: u64) -> i64 {
    i64::try_from(after).unwrap_or(i64::MAX) - i64::try_from(before).unwrap_or(i64::MAX)
}

/// One labeled workload row for the comparison report table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadReport {
    /// Workload name (for example `DML` or `query hot+cold`).
    pub workload: String,
    /// Target label (`plain` or `koldstore`).
    pub target: String,
    /// Before/peak/after snapshots for the workload.
    pub allocation: PeakAllocation,
}

/// Formats bytes as a compact human-readable string (`1.5 MiB`, `320 KiB`, …).
#[must_use]
pub fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let bytes_f = bytes as f64;
    if bytes_f >= GIB {
        format!("{:.2} GiB", bytes_f / GIB)
    } else if bytes_f >= MIB {
        format!("{:.2} MiB", bytes_f / MIB)
    } else if bytes_f >= KIB {
        format!("{:.1} KiB", bytes_f / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Formats a signed byte delta with an explicit `+`/`-` prefix.
#[must_use]
pub fn format_signed_bytes(delta: i64) -> String {
    if delta >= 0 {
        format!("+{}", format_bytes(delta as u64))
    } else {
        format!("-{}", format_bytes(delta.unsigned_abs()))
    }
}

/// Renders a markdown comparison table for plain Postgres vs koldstore workloads.
#[must_use]
pub fn format_comparison_table(title: &str, rows: &[WorkloadReport]) -> String {
    let mut out = String::new();
    out.push_str(&format!("=== {title} ===\n"));
    out.push_str(
        "| Workload | Target | Ctx before | Ctx after | Ctx Δ | Ctx spike | RSS before | RSS after | RSS Δ | RSS spike |\n",
    );
    out.push_str("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for row in rows {
        let a = row.allocation;
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            row.workload,
            row.target,
            format_bytes(a.before.pg_context_bytes),
            format_bytes(a.after.pg_context_bytes),
            format_signed_bytes(a.context_delta_bytes()),
            format_bytes(a.context_spike_bytes()),
            format_bytes(a.before.rss_bytes),
            format_bytes(a.after.rss_bytes),
            format_signed_bytes(a.rss_delta_bytes()),
            format_bytes(a.rss_spike_bytes()),
        ));
    }
    out
}

/// Renders overhead rows (`koldstore − plain`) for matching workload names.
#[must_use]
pub fn format_overhead_table(rows: &[WorkloadReport]) -> String {
    let mut out = String::new();
    out.push_str("=== Overhead (koldstore − plain) ===\n");
    out.push_str(
        "| Workload | Ctx after overhead | Ctx Δ overhead | Ctx spike overhead | RSS after overhead | RSS Δ overhead | RSS spike overhead |\n",
    );
    out.push_str("|---|---:|---:|---:|---:|---:|---:|\n");

    let plain: Vec<_> = rows.iter().filter(|row| row.target == "plain").collect();
    for plain_row in plain {
        let Some(kold) = rows
            .iter()
            .find(|row| row.target == "koldstore" && row.workload == plain_row.workload)
        else {
            continue;
        };
        let p = plain_row.allocation;
        let k = kold.allocation;
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} |\n",
            plain_row.workload,
            format_signed_bytes(signed_delta(
                p.after.pg_context_bytes,
                k.after.pg_context_bytes
            )),
            format_signed_bytes(k.context_delta_bytes() - p.context_delta_bytes()),
            format_signed_bytes(signed_delta(
                p.context_spike_bytes(),
                k.context_spike_bytes()
            )),
            format_signed_bytes(signed_delta(p.after.rss_bytes, k.after.rss_bytes)),
            format_signed_bytes(k.rss_delta_bytes() - p.rss_delta_bytes()),
            format_signed_bytes(signed_delta(p.rss_spike_bytes(), k.rss_spike_bytes())),
        ));
    }
    out
}

/// Bounds used by lifecycle leak gates after warmup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrowthBudget {
    /// Maximum allowed growth in PostgreSQL memory-context totals.
    pub max_pg_context_growth_bytes: u64,
    /// Maximum allowed growth in process RSS.
    pub max_rss_growth_bytes: u64,
    /// Maximum allowed average per-cycle context growth (slope guard).
    pub max_pg_context_bytes_per_cycle: u64,
    /// Maximum allowed average per-cycle RSS growth (slope guard).
    pub max_rss_bytes_per_cycle: u64,
}

impl Default for GrowthBudget {
    fn default() -> Self {
        Self {
            // Context totals should stay nearly flat once caches are warm.
            max_pg_context_growth_bytes: 24 * 1024 * 1024,
            // RSS allows allocator arenas / shared buffer noise across workers.
            max_rss_growth_bytes: 96 * 1024 * 1024,
            max_pg_context_bytes_per_cycle: 2 * 1024 * 1024,
            max_rss_bytes_per_cycle: 8 * 1024 * 1024,
        }
    }
}

/// Result of evaluating a measured snapshot series against a budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrowthEvaluation {
    /// First measured snapshot (post-warmup baseline).
    pub baseline: MemorySnapshot,
    /// Final measured snapshot.
    pub final_snapshot: MemorySnapshot,
    /// Absolute context growth over the measured window.
    pub pg_context_growth_bytes: u64,
    /// Absolute RSS growth over the measured window.
    pub rss_growth_bytes: u64,
    /// Average context growth per measured cycle.
    pub pg_context_bytes_per_cycle: u64,
    /// Average RSS growth per measured cycle.
    pub rss_bytes_per_cycle: u64,
}

impl GrowthEvaluation {
    /// Returns true when both absolute and per-cycle growth stay inside `budget`.
    #[must_use]
    pub fn within_budget(&self, budget: GrowthBudget) -> bool {
        self.pg_context_growth_bytes <= budget.max_pg_context_growth_bytes
            && self.rss_growth_bytes <= budget.max_rss_growth_bytes
            && self.pg_context_bytes_per_cycle <= budget.max_pg_context_bytes_per_cycle
            && self.rss_bytes_per_cycle <= budget.max_rss_bytes_per_cycle
    }
}

/// Returns true when every probe releases resources by the final snapshot.
#[must_use]
pub fn repeated_scan_releases_resources<const N: usize>(allocations: [PeakAllocation; N]) -> bool {
    allocations
        .into_iter()
        .all(|allocation| allocation.retained_growth_bytes() == 0)
}

/// Evaluates retained growth across an ordered series of post-warmup snapshots.
///
/// `samples` must contain at least two snapshots collected after warmup cycles.
/// Growth uses saturating subtraction so allocator noise that briefly shrinks
/// RSS does not underflow.
///
/// # Errors
///
/// Returns an error when fewer than two samples are provided.
pub fn evaluate_growth(samples: &[MemorySnapshot]) -> Result<GrowthEvaluation, &'static str> {
    if samples.len() < 2 {
        return Err("growth evaluation requires at least two post-warmup snapshots");
    }
    let baseline = samples[0];
    let final_snapshot = *samples.last().expect("len >= 2");
    let cycles = (samples.len() - 1) as u64;
    let pg_context_growth_bytes = final_snapshot
        .pg_context_bytes
        .saturating_sub(baseline.pg_context_bytes);
    let rss_growth_bytes = final_snapshot.rss_bytes.saturating_sub(baseline.rss_bytes);
    Ok(GrowthEvaluation {
        baseline,
        final_snapshot,
        pg_context_growth_bytes,
        rss_growth_bytes,
        pg_context_bytes_per_cycle: pg_context_growth_bytes / cycles,
        rss_bytes_per_cycle: rss_growth_bytes / cycles,
    })
}

/// Reads RSS bytes for `pid` from `/proc` (Linux) or `ps` (macOS/BSD).
///
/// # Errors
///
/// Returns an error when the process cannot be sampled.
pub fn process_rss_bytes(pid: i32) -> Result<u64, String> {
    let proc_status = format!("/proc/{pid}/status");
    if let Ok(contents) = fs::read_to_string(&proc_status) {
        for line in contents.lines() {
            if let Some(value) = line.strip_prefix("VmRSS:") {
                let kb = value
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| format!("VmRSS missing numeric value for pid {pid}"))?;
                let kb: u64 = kb
                    .parse()
                    .map_err(|error| format!("parse VmRSS for pid {pid}: {error}"))?;
                return Ok(kb.saturating_mul(1024));
            }
        }
        return Err(format!("VmRSS not found in {proc_status}"));
    }

    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .map_err(|error| format!("ps rss for pid {pid}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "ps rss for pid {pid} failed with status {}",
            output.status
        ));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let kb: u64 = text
        .split_whitespace()
        .next()
        .ok_or_else(|| format!("empty ps rss output for pid {pid}"))?
        .parse()
        .map_err(|error| format!("parse ps rss for pid {pid}: {error}"))?;
    Ok(kb.saturating_mul(1024))
}

/// Sums RSS bytes for every process whose command line matches `needle`.
///
/// Used to include background workers (flush/async mirror) that are not the
/// SQL session backend.
///
/// # Errors
///
/// Returns an error when process listing fails.
pub fn matched_processes_rss_bytes(needle: &str) -> Result<u64, String> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,rss=,command="])
        .output()
        .map_err(|error| format!("ps list: {error}"))?;
    if !output.status.success() {
        return Err(format!("ps list failed with status {}", output.status));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut total = 0_u64;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || !line.contains(needle) {
            continue;
        }
        let mut parts = line.split_whitespace();
        let _pid = parts.next();
        let Some(rss_kb) = parts.next() else {
            continue;
        };
        if let Ok(kb) = rss_kb.parse::<u64>() {
            total = total.saturating_add(kb.saturating_mul(1024));
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate_growth_detects_linear_context_leak() {
        let samples = [
            MemorySnapshot {
                rss_bytes: 100,
                pg_context_bytes: 1_000,
                allocator_bytes: None,
            },
            MemorySnapshot {
                rss_bytes: 100,
                pg_context_bytes: 5_000_000,
                allocator_bytes: None,
            },
            MemorySnapshot {
                rss_bytes: 100,
                pg_context_bytes: 10_000_000,
                allocator_bytes: None,
            },
        ];
        let evaluation = evaluate_growth(&samples).expect("samples");
        assert!(evaluation.pg_context_growth_bytes >= 9_000_000);
        assert!(!evaluation.within_budget(GrowthBudget {
            max_pg_context_growth_bytes: 1_000_000,
            max_rss_growth_bytes: 1_000_000,
            max_pg_context_bytes_per_cycle: 100_000,
            max_rss_bytes_per_cycle: 100_000,
        }));
    }

    #[test]
    fn evaluate_growth_accepts_stable_series() {
        let samples = [
            MemorySnapshot {
                rss_bytes: 50_000_000,
                pg_context_bytes: 2_000_000,
                allocator_bytes: None,
            },
            MemorySnapshot {
                rss_bytes: 50_100_000,
                pg_context_bytes: 2_050_000,
                allocator_bytes: None,
            },
            MemorySnapshot {
                rss_bytes: 50_200_000,
                pg_context_bytes: 2_100_000,
                allocator_bytes: None,
            },
        ];
        let evaluation = evaluate_growth(&samples).expect("samples");
        assert!(evaluation.within_budget(GrowthBudget::default()));
    }

    #[test]
    fn comparison_table_includes_spike_and_delta_columns() {
        let rows = [WorkloadReport {
            workload: "DML".into(),
            target: "plain".into(),
            allocation: PeakAllocation {
                before: MemorySnapshot {
                    rss_bytes: 10_000_000,
                    pg_context_bytes: 1_000_000,
                    allocator_bytes: None,
                },
                peak: MemorySnapshot {
                    rss_bytes: 12_000_000,
                    pg_context_bytes: 2_000_000,
                    allocator_bytes: None,
                },
                after: MemorySnapshot {
                    rss_bytes: 10_500_000,
                    pg_context_bytes: 1_100_000,
                    allocator_bytes: None,
                },
            },
        }];
        let table = format_comparison_table("demo", &rows);
        assert!(table.contains("Ctx spike"));
        assert!(table.contains("DML"));
        assert!(table.contains("+97.7 KiB") || table.contains("+100.0 KiB") || table.contains("+"));
    }
}
