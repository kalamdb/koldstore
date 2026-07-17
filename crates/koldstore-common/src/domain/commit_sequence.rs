//! Commit-order allocation domain keys for transaction-scoped sequencing.
//!
//! Pure naming and advisory-lock key derivation. PostgreSQL lock acquisition
//! stays in `pg_koldstore::hooks`.

use crate::ScopeKey;

/// Advisory lock namespace for transaction commit-order allocation.
pub const COMMIT_SEQUENCE_LOCK_NAMESPACE: &str = "pg_koldstore.commit_sequence";

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

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

#[cfg(test)]
mod tests {
    use super::CommitSequenceDomain;
    use crate::ScopeKey;

    #[test]
    fn domain_keys_are_stable_and_scope_sensitive() {
        let shared = CommitSequenceDomain::for_table_scope(42, None);
        let scoped = CommitSequenceDomain::for_table_scope(
            42,
            Some(ScopeKey::new("pk:abc").expect("scope key")),
        );
        assert_eq!(
            shared.advisory_lock_key(),
            CommitSequenceDomain::for_table_scope(42, None).advisory_lock_key()
        );
        assert_ne!(shared.advisory_lock_key(), scoped.advisory_lock_key());
        assert!(shared.name().contains("scope:shared"));
        assert!(scoped.name().contains("scope:pk:abc"));
    }
}
