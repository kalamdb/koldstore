//! Process-heap release after large temporary Rust allocations.
//!
//! PostgreSQL `palloc` contexts are separate; this targets the Rust global
//! allocator (glibc `malloc` on Linux GNU) used by Arrow/Parquet/object_store.

use std::cell::Cell;

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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{mark_heap_trim_pending, release_process_heap_if_pending, HEAP_TRIM_PENDING};

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
