//! Transaction-scoped commit sequence allocation.

use std::sync::{
    atomic::{AtomicI64, Ordering},
    Mutex,
};

use koldstore_core::{CommitSeq, Result};

static NEXT_COMMIT_SEQ: AtomicI64 = AtomicI64::new(1);

/// Allocates a process-local commit sequence for non-pgrx tests.
///
/// PostgreSQL builds replace this with advisory-lock-backed allocation.
pub fn allocate_commit_seq_for_tests() -> Result<CommitSeq> {
    CommitSeq::new(NEXT_COMMIT_SEQ.fetch_add(1, Ordering::SeqCst))
}

/// Commit sequence allocator abstraction.
///
/// PostgreSQL builds use an advisory-lock-backed transaction domain. The test
/// shell preserves monotonic allocation and records the active domain.
#[derive(Debug)]
pub struct CommitSequenceAllocator {
    next: AtomicI64,
    domain: Mutex<String>,
}

impl CommitSequenceAllocator {
    /// Creates a test allocator.
    #[must_use]
    pub fn new_for_tests() -> Self {
        Self {
            next: AtomicI64::new(1),
            domain: Mutex::new(String::new()),
        }
    }

    /// Allocates a commit sequence for a commit-order domain.
    ///
    /// # Errors
    ///
    /// Returns an error when the generated sequence is invalid.
    pub fn allocate_for_domain(&self, domain: &str) -> Result<CommitSeq> {
        if let Ok(mut current) = self.domain.lock() {
            current.clear();
            current.push_str(domain);
        }
        CommitSeq::new(self.next.fetch_add(1, Ordering::SeqCst))
    }

    /// Returns the most recent domain name.
    #[must_use]
    pub fn domain(&self) -> String {
        self.domain
            .lock()
            .map(|domain| domain.clone())
            .unwrap_or_default()
    }
}
