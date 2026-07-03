//! Memory allocation baseline helpers for E2E and benchmark probes.

/// Snapshot of process and PostgreSQL memory state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemorySnapshot {
    /// Resident set size in bytes.
    pub rss_bytes: u64,
    /// PostgreSQL memory-context total bytes.
    pub pg_context_bytes: u64,
    /// Allocator-reported bytes when available.
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

/// Captures per-scan peak allocation.
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
    /// Returns allocation growth after the scan.
    #[must_use]
    pub fn retained_growth_bytes(self) -> u64 {
        self.after
            .known_total_bytes()
            .saturating_sub(self.before.known_total_bytes())
    }
}

/// Returns true when repeated scan probes release all resources by the final snapshot.
#[must_use]
pub fn repeated_scan_releases_resources<const N: usize>(allocations: [PeakAllocation; N]) -> bool {
    allocations
        .into_iter()
        .all(|allocation| allocation.retained_growth_bytes() == 0)
}
