//! Index DDL plans for internal `koldstore` tables.
//!
//! Lists required indexes and uniqueness constraints for the setup catalog. The
//! SQL file remains the executable install artifact, while these specs make
//! index ownership and duplicate checks explicit.

use crate::bootstrap::{BootstrapObjectKind, BootstrapPlan};

/// Required internal catalog index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogIndexSpec {
    /// Index name.
    pub name: &'static str,
    /// Schema-qualified table indexed by this object.
    pub table: &'static str,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Maintainer-facing reason the index exists.
    pub purpose: &'static str,
}

/// Internal catalog indexes installed by the extension.
pub const REQUIRED_CATALOG_INDEXES: &[CatalogIndexSpec] = &[
    CatalogIndexSpec {
        name: "schemas_one_active_per_table_idx",
        table: "koldstore.schemas",
        unique: true,
        purpose: "one active schema version per managed table",
    },
    CatalogIndexSpec {
        name: "manifest_dirty_idx",
        table: "koldstore.manifest",
        unique: false,
        purpose: "manifest repair scans over dirty entries",
    },
    CatalogIndexSpec {
        name: "manifest_scope_lookup_idx",
        table: "koldstore.manifest",
        unique: false,
        purpose: "user-scope manifest lookups",
    },
    CatalogIndexSpec {
        name: "jobs_pending_idx",
        table: "koldstore.jobs",
        unique: false,
        purpose: "legacy pending/running job lookup compatibility",
    },
    CatalogIndexSpec {
        name: "jobs_claimable_idx",
        table: "koldstore.jobs",
        unique: false,
        purpose: "lease-aware job claiming across job types",
    },
    CatalogIndexSpec {
        name: "jobs_claimable_by_type_idx",
        table: "koldstore.jobs",
        unique: false,
        purpose: "lease-aware claiming for one job type",
    },
    CatalogIndexSpec {
        name: "jobs_running_lease_idx",
        table: "koldstore.jobs",
        unique: false,
        purpose: "stale running lease recovery",
    },
    CatalogIndexSpec {
        name: "jobs_one_active_flush_per_scope_idx",
        table: "koldstore.jobs",
        unique: true,
        purpose: "single active flush job per table/scope",
    },
    CatalogIndexSpec {
        name: "jobs_one_active_migration_per_table_idx",
        table: "koldstore.jobs",
        unique: true,
        purpose: "single active migration backfill per table",
    },
    CatalogIndexSpec {
        name: "cold_segments_active_scope_seq_idx",
        table: "koldstore.cold_segments",
        unique: false,
        purpose: "merge scans by table, scope, and sequence range",
    },
    CatalogIndexSpec {
        name: "cold_segments_active_commit_idx",
        table: "koldstore.cold_segments",
        unique: false,
        purpose: "commit-sequence pruning for active cold data",
    },
];

/// Returns required catalog indexes missing from a parsed bootstrap plan.
#[must_use]
pub fn missing_catalog_indexes(plan: &BootstrapPlan) -> Vec<&'static CatalogIndexSpec> {
    REQUIRED_CATALOG_INDEXES
        .iter()
        .filter(|index| !plan.contains_object(BootstrapObjectKind::Index, index.name))
        .collect()
}
