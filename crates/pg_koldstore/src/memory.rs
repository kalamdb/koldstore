//! PostgreSQL memory-context ownership helpers and process-heap release.

use std::cell::Cell;

/// Memory owner labels used in tracing and diagnostics.
pub const MEMORY_OWNER_LABELS: &[&str] = &[
    "ffi",
    "scan_state",
    "spi_tuple",
    "arrow_buffer",
    "object_store_handle",
];

thread_local! {
    /// Set after merge-scan / flush spikes so `ExecutorEnd` can reclaim arenas.
    static HEAP_TRIM_PENDING: Cell<bool> = const { Cell::new(false) };
}

/// Marks this backend so the next [`release_process_heap_if_pending`] trims.
///
/// Call after work that allocates large Rust/Arrow/Parquet buffers (merge scan,
/// flush). Connection pools often keep backends alive; without a trim, glibc
/// retains free huge pages and idle RSS stays hundreds of MB per backend.
pub fn mark_heap_trim_pending() {
    HEAP_TRIM_PENDING.with(|pending| pending.set(true));
}

/// Returns free glibc heap pages to the OS when a prior spike marked a trim.
///
/// No-op when nothing marked a trim, so ordinary `SELECT 1` keepalives stay cheap.
pub fn release_process_heap_if_pending() {
    if HEAP_TRIM_PENDING.with(|pending| pending.replace(false)) {
        release_process_heap();
    }
}

/// Asks the process allocator to return free pages after large temporary spikes.
///
/// PostgreSQL `palloc` contexts are separate; this targets the Rust global
/// allocator (glibc `malloc` on Linux GNU) used by Arrow/Parquet/object_store.
pub fn release_process_heap() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        extern "C" {
            fn malloc_trim(pad: usize) -> std::os::raw::c_int;
        }
        unsafe {
            let _ = malloc_trim(0);
        }
    }
}

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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{
        mark_heap_trim_pending, release_process_heap_if_pending, MemoryOwner, HEAP_TRIM_PENDING,
    };

    #[test]
    fn memory_owner_tracks_and_resets() {
        let mut owner = MemoryOwner::new("scan_state");
        owner.track_allocation(128);
        assert_eq!(owner.allocated_bytes(), 128);
        owner.reset();
        assert_eq!(owner.allocated_bytes(), 0);
    }

    #[test]
    fn heap_trim_pending_is_one_shot() {
        HEAP_TRIM_PENDING.with(|pending| pending.set(false));
        release_process_heap_if_pending();
        assert!(!HEAP_TRIM_PENDING.with(Cell::get));

        mark_heap_trim_pending();
        assert!(HEAP_TRIM_PENDING.with(Cell::get));
        release_process_heap_if_pending();
        assert!(!HEAP_TRIM_PENDING.with(Cell::get));
        // Second call must not panic and must stay clear.
        release_process_heap_if_pending();
        assert!(!HEAP_TRIM_PENDING.with(Cell::get));
    }
}
