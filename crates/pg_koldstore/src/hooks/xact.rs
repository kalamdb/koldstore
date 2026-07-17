//! Transaction-scoped commit sequence allocation.

use std::sync::{
    atomic::{AtomicI64, Ordering},
    Mutex,
};

use koldstore_common::{CommitSeq, Result};

pub use koldstore_common::{CommitSequenceDomain, COMMIT_SEQUENCE_LOCK_NAMESPACE};

static NEXT_COMMIT_SEQ: AtomicI64 = AtomicI64::new(1);

/// Transaction coupling mode for mirror capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorCaptureTransactionScope {
    /// Mirror mutation executes as an ordinary row trigger in the user's transaction.
    SameUserTransaction,
}

/// Returns the clean-schema mirror capture transaction scope.
#[must_use]
pub const fn mirror_capture_transaction_scope() -> MirrorCaptureTransactionScope {
    MirrorCaptureTransactionScope::SameUserTransaction
}

/// Returns whether a capture scope rolls back with the user DML.
#[must_use]
pub const fn mirror_capture_rolls_back_with_user_transaction(
    scope: MirrorCaptureTransactionScope,
) -> bool {
    matches!(scope, MirrorCaptureTransactionScope::SameUserTransaction)
}

/// Allocates a process-local commit sequence for non-pgrx tests.
///
/// PostgreSQL builds replace this with advisory-lock-backed allocation.
pub fn allocate_commit_seq_for_tests() -> Result<CommitSeq> {
    CommitSeq::new(NEXT_COMMIT_SEQ.fetch_add(1, Ordering::SeqCst))
}

/// Result of acquiring the transaction commit-order lock and allocating a cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSequenceAllocation {
    /// Allocated commit-order cursor.
    pub commit_seq: CommitSeq,
    /// Advisory lock key acquired for the transaction.
    pub lock_key: i64,
    /// Human-readable allocation domain.
    pub domain_name: String,
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
    pub fn allocate_for_domain(
        &self,
        domain: &CommitSequenceDomain,
    ) -> Result<CommitSequenceAllocation> {
        if let Ok(mut current) = self.domain.lock() {
            current.clear();
            current.push_str(domain.name());
        }
        let commit_seq = CommitSeq::new(self.next.fetch_add(1, Ordering::SeqCst))?;
        Ok(CommitSequenceAllocation {
            commit_seq,
            lock_key: domain.advisory_lock_key(),
            domain_name: domain.name().to_string(),
        })
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
