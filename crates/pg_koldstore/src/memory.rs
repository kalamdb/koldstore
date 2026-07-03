//! PostgreSQL memory-context ownership helpers.

/// Memory owner labels used in tracing and diagnostics.
pub const MEMORY_OWNER_LABELS: &[&str] = &[
    "ffi",
    "scan_state",
    "spi_tuple",
    "arrow_buffer",
    "object_store_handle",
];

/// Testable memory owner accounting for PostgreSQL memory-context boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOwner {
    label: String,
    allocated_bytes: usize,
}

impl MemoryOwner {
    /// Creates a memory owner label.
    #[must_use]
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            allocated_bytes: 0,
        }
    }

    /// Returns the owner label.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Tracks an allocation under this owner.
    pub fn track_allocation(&mut self, bytes: usize) {
        self.allocated_bytes = self.allocated_bytes.saturating_add(bytes);
    }

    /// Returns tracked bytes.
    #[must_use]
    pub const fn allocated_bytes(&self) -> usize {
        self.allocated_bytes
    }

    /// Resets tracked allocations after memory context cleanup.
    pub fn reset(&mut self) {
        self.allocated_bytes = 0;
    }
}
