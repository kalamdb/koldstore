//! Managed table metadata.

use serde::{Deserialize, Serialize};

use koldstore_core::{Diagnostic, KoldstoreError, Result, TableKind};

/// Flush policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushPolicy {
    pub rows: Option<u64>,
    pub interval_seconds: Option<u64>,
}

/// FK migration policy classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FkPolicyDecision {
    /// Safe because no risky FKs exist or flush is disabled.
    Allow,
    /// Allowed only because the operator explicitly accepted hot-only FK semantics.
    AllowHotOnly,
    /// Reject because native FK checks cannot see cold rows.
    Reject,
}

impl FkPolicyDecision {
    /// Classifies FK policy for migration.
    #[must_use]
    pub const fn classify(
        has_inbound_fk: bool,
        has_outbound_fk: bool,
        flush_enabled: bool,
        allow_hot_only: bool,
    ) -> Self {
        if !flush_enabled || (!has_inbound_fk && !has_outbound_fk) {
            Self::Allow
        } else if allow_hot_only {
            Self::AllowHotOnly
        } else {
            Self::Reject
        }
    }
}

/// Managed table metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedTableMeta {
    pub table_oid: u32,
    pub table_kind: TableKind,
    pub scope_column: Option<String>,
    pub flush_policy: Option<FlushPolicy>,
}

impl ManagedTableMeta {
    /// Validates table metadata invariants.
    ///
    /// # Errors
    ///
    /// Returns an error when a user-scoped table has no scope column.
    pub fn validate(&self) -> Result<()> {
        if self.table_kind == TableKind::User
            && self
                .scope_column
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
        {
            return Err(KoldstoreError::CatalogValidation {
                diagnostic: Diagnostic::new(
                    "missing_scope_column",
                    "user-scoped managed tables require a scope column",
                ),
            });
        }

        Ok(())
    }
}
