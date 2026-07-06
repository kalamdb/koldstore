//! Managed table metadata.

use serde::{Deserialize, Serialize};

use koldstore_common::{Diagnostic, KoldstoreError, PrimaryKeyShape, Result, TableKind};
use koldstore_schema::MirrorInitializationState;

/// Flush policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushPolicy {
    pub row_limit: Option<u64>,
    pub duration_seconds: Option<u64>,
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
    pub mirror_relation: Option<String>,
    pub primary_key_shape: Option<PrimaryKeyShape>,
    pub initialization_state: MirrorInitializationState,
    pub flush_policy: Option<FlushPolicy>,
    pub schema_version: u32,
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

        if self
            .mirror_relation
            .as_deref()
            .is_some_and(|relation| relation.trim().is_empty())
        {
            return Err(KoldstoreError::CatalogValidation {
                diagnostic: Diagnostic::new(
                    "blank_mirror_relation",
                    "managed mirror relation cannot be blank",
                ),
            });
        }

        if self.initialization_state == MirrorInitializationState::Complete
            && self.primary_key_shape.is_none()
        {
            return Err(KoldstoreError::CatalogValidation {
                diagnostic: Diagnostic::new(
                    "missing_primary_key_shape",
                    "completed clean-schema metadata requires primary-key shape",
                ),
            });
        }

        Ok(())
    }
}
