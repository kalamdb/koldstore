//! Transaction-scoped commit sequence allocation.

use std::sync::{
    atomic::{AtomicI64, Ordering},
    Mutex,
};

use koldstore_core::{CommitSeq, Result, ScopeKey};
#[cfg(feature = "pg")]
use koldstore_core::{Diagnostic, KoldstoreError};

static NEXT_COMMIT_SEQ: AtomicI64 = AtomicI64::new(1);
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Advisory lock namespace for transaction commit-order allocation.
pub const COMMIT_SEQUENCE_LOCK_NAMESPACE: &str = "pg_koldstore.commit_sequence";

/// Allocates a process-local commit sequence for non-pgrx tests.
///
/// PostgreSQL builds replace this with advisory-lock-backed allocation.
pub fn allocate_commit_seq_for_tests() -> Result<CommitSeq> {
    CommitSeq::new(NEXT_COMMIT_SEQ.fetch_add(1, Ordering::SeqCst))
}

/// Commit-order allocation domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSequenceDomain {
    table_oid: u32,
    scope_key: Option<ScopeKey>,
    name: String,
    advisory_lock_key: i64,
}

impl CommitSequenceDomain {
    /// Builds a transaction commit-order domain for a managed table and scope.
    #[must_use]
    pub fn for_table_scope(table_oid: u32, scope_key: Option<ScopeKey>) -> Self {
        let normalized_scope = scope_key.as_ref().map(ScopeKey::as_str);
        let name = match normalized_scope {
            Some(scope) => format!("table:{table_oid}:scope:{scope}"),
            None => format!("table:{table_oid}:scope:shared"),
        };
        let advisory_lock_key = advisory_lock_key(&name);

        Self {
            table_oid,
            scope_key,
            name,
            advisory_lock_key,
        }
    }

    /// Returns the table oid.
    #[must_use]
    pub const fn table_oid(&self) -> u32 {
        self.table_oid
    }

    /// Returns the optional scope key.
    #[must_use]
    pub fn scope_key(&self) -> Option<&ScopeKey> {
        self.scope_key.as_ref()
    }

    /// Returns a stable diagnostic domain name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the signed PostgreSQL advisory-lock key.
    #[must_use]
    pub const fn advisory_lock_key(&self) -> i64 {
        self.advisory_lock_key
    }
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

/// Allocates a commit sequence under a PostgreSQL transaction-scoped advisory lock.
///
/// # Errors
///
/// Returns an error when PostgreSQL SPI cannot acquire the lock or sequence.
#[cfg(feature = "pg")]
pub fn allocate_for_current_transaction(
    domain: &CommitSequenceDomain,
) -> Result<CommitSequenceAllocation> {
    let lock_key = domain.advisory_lock_key();
    let lock_sql = format!("SELECT pg_advisory_xact_lock({lock_key})");
    pgrx::Spi::run(&lock_sql).map_err(|error| catalog_error("commit_lock_failed", error))?;

    let commit_seq = pgrx::Spi::get_one::<i64>(
        "SELECT nextval('koldstore.global_commit_seq'::regclass)::bigint",
    )
    .map_err(|error| catalog_error("commit_seq_failed", error))?
    .ok_or_else(|| KoldstoreError::CatalogValidation {
        diagnostic: Diagnostic::new(
            "commit_seq_missing",
            "global commit sequence returned no value",
        ),
    })
    .and_then(CommitSeq::new)?;

    Ok(CommitSequenceAllocation {
        commit_seq,
        lock_key,
        domain_name: domain.name().to_string(),
    })
}

#[cfg(feature = "pg")]
fn catalog_error(code: &'static str, error: impl std::fmt::Display) -> KoldstoreError {
    KoldstoreError::CatalogValidation {
        diagnostic: Diagnostic::new(code, error.to_string()),
    }
}

fn advisory_lock_key(domain_name: &str) -> i64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in COMMIT_SEQUENCE_LOCK_NAMESPACE
        .bytes()
        .chain([0])
        .chain(domain_name.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    i64::from_ne_bytes(hash.to_ne_bytes())
}
