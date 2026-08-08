//! Process-heap release after large temporary Rust allocations.
//!
//! PostgreSQL `palloc` contexts are separate; this targets the Rust global
//! allocator (glibc `malloc` on Linux GNU) used by Arrow/Parquet/object_store.
//! Heap trimming is intentionally coalesced: `malloc_trim(0)` walks allocator
//! arenas and should not run after every bounded flush pass.

use std::cell::Cell;
use std::time::{Duration, Instant};

/// Minimum spacing between allocator-wide trim attempts in one backend.
///
/// A one-shot background worker naturally returns all memory on process exit;
/// pooled PostgreSQL backends still retry a pending trim at later ExecutorEnd
/// boundaries. Two seconds prevents multi-pass flushes from repeatedly paying
/// allocator-global work while retaining the original idle-RSS benefit.
const HEAP_TRIM_MIN_INTERVAL: Duration = Duration::from_secs(2);

thread_local! {
    /// Set after merge-scan / flush spikes so a safe boundary can reclaim arenas.
    static HEAP_TRIM_PENDING: Cell<bool> = const { Cell::new(false) };
    /// Last successful/attempted allocator trim in this backend.
    static LAST_HEAP_TRIM_AT: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Marks this backend so the next [`release_process_heap_if_pending`] can trim.
///
/// Call after work that allocates large Rust/Arrow/Parquet buffers (merge scan,
/// flush). Connection pools often keep backends alive; without a trim, glibc
/// retains free huge pages and idle RSS stays hundreds of MB per backend.
pub fn mark_heap_trim_pending() {
    HEAP_TRIM_PENDING.with(|pending| pending.set(true));
}

/// Returns free glibc heap pages to the OS when a prior spike marked a trim.
///
/// Repeated calls inside the coalescing window keep the pending bit set so a
/// later statement boundary may try again; ordinary `SELECT 1` keepalives with
/// no prior large allocation stay a single cheap thread-local read.
pub fn release_process_heap_if_pending() {
    if !HEAP_TRIM_PENDING.with(Cell::get) {
        return;
    }
    if try_release_process_heap() {
        HEAP_TRIM_PENDING.with(|pending| pending.set(false));
    }
}

/// Requests allocator page release after a large temporary spike.
///
/// This API remains safe to call at multiple flush-pass boundaries. At most one
/// allocator-wide trim is executed per [`HEAP_TRIM_MIN_INTERVAL`]; suppressed
/// attempts retain the pending marker for a later backend boundary.
pub fn release_process_heap() {
    mark_heap_trim_pending();
    if try_release_process_heap() {
        HEAP_TRIM_PENDING.with(|pending| pending.set(false));
    }
}

fn try_release_process_heap() -> bool {
    let now = Instant::now();
    let due = LAST_HEAP_TRIM_AT.with(|last| {
        if last
            .get()
            .is_some_and(|previous| now.duration_since(previous) < HEAP_TRIM_MIN_INTERVAL)
        {
            false
        } else {
            last.set(Some(now));
            true
        }
    });
    if !due {
        return false;
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        extern "C" {
            fn malloc_trim(pad: usize) -> std::os::raw::c_int;
        }
        unsafe {
            let _ = malloc_trim(0);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{
        mark_heap_trim_pending, release_process_heap_if_pending, HEAP_TRIM_PENDING,
        LAST_HEAP_TRIM_AT,
    };

    #[test]
    fn heap_trim_pending_is_cleared_after_due_trim() {
        HEAP_TRIM_PENDING.with(|pending| pending.set(false));
        LAST_HEAP_TRIM_AT.with(|last| last.set(None));
        release_process_heap_if_pending();
        assert!(!HEAP_TRIM_PENDING.with(Cell::get));

        mark_heap_trim_pending();
        assert!(HEAP_TRIM_PENDING.with(Cell::get));
        release_process_heap_if_pending();
        assert!(!HEAP_TRIM_PENDING.with(Cell::get));
    }

    #[test]
    fn rapid_second_trim_stays_pending_for_later_boundary() {
        HEAP_TRIM_PENDING.with(|pending| pending.set(false));
        LAST_HEAP_TRIM_AT.with(|last| last.set(None));

        mark_heap_trim_pending();
        release_process_heap_if_pending();
        assert!(!HEAP_TRIM_PENDING.with(Cell::get));

        mark_heap_trim_pending();
        release_process_heap_if_pending();
        assert!(HEAP_TRIM_PENDING.with(Cell::get));
    }
}
