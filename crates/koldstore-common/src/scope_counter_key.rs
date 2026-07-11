//! Process-local flush counter key: table + optional scope.
//!
//! Shared and user-scoped tables use the same key shape. Shared tables pass
//! `scope_value: None`; user tables pass the configured scope column value.

use serde::{Deserialize, Serialize};

use crate::{Result, ScopeKey};

/// Identity for in-memory flush initiation counters.
///
/// Keyed as `(table_oid, Optional<scope>)` so User and Shared tables share one
/// mechanism (FR-026).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ScopeCounterKey {
    /// Managed PostgreSQL table OID.
    pub table_oid: u32,
    /// Present for user-scoped tables; absent for shared tables.
    pub scope_value: Option<ScopeKey>,
}

impl ScopeCounterKey {
    /// Builds a shared-table key (`scope_value = None`).
    #[must_use]
    pub const fn shared(table_oid: u32) -> Self {
        Self {
            table_oid,
            scope_value: None,
        }
    }

    /// Builds a user-scoped key from a non-empty scope string.
    ///
    /// # Errors
    ///
    /// Returns an error when `scope` is empty or whitespace-only.
    pub fn scoped(table_oid: u32, scope: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            table_oid,
            scope_value: Some(ScopeKey::new(scope)?),
        })
    }

    /// Builds a key from an optional scope (shared when `None` or blank).
    ///
    /// # Errors
    ///
    /// Returns an error when `scope` is `Some` but empty/whitespace-only.
    pub fn from_optional_scope(table_oid: u32, scope: Option<&str>) -> Result<Self> {
        match scope.map(str::trim).filter(|value| !value.is_empty()) {
            None => Ok(Self::shared(table_oid)),
            Some(value) => Self::scoped(table_oid, value),
        }
    }

    /// Catalog `scope_key` text: empty string for shared, otherwise the scope value.
    #[must_use]
    pub fn catalog_scope_key(&self) -> &str {
        self.scope_value
            .as_ref()
            .map(ScopeKey::as_str)
            .unwrap_or("")
    }

    /// Returns true when this key represents a shared (unscoped) table stream.
    #[must_use]
    pub const fn is_shared(&self) -> bool {
        self.scope_value.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_and_scoped_keys_differ() {
        let shared = ScopeCounterKey::shared(42);
        let scoped = ScopeCounterKey::scoped(42, "tenant-a").expect("scope");
        assert!(shared.is_shared());
        assert!(!scoped.is_shared());
        assert_eq!(shared.catalog_scope_key(), "");
        assert_eq!(scoped.catalog_scope_key(), "tenant-a");
        assert_ne!(shared, scoped);
    }
}
