//! DDL plans for internal catalog tables (`storage`, `manifest`, `jobs`, etc.).
//!
//! Lists the extension-owned catalog tables that must appear in the bootstrap
//! plan. These names are the typed contract used by setup tests and by future
//! DDL generation work.

use crate::bootstrap::{BootstrapObjectKind, BootstrapPlan};

/// Required internal catalog table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogTableSpec {
    /// Schema-qualified table name.
    pub name: &'static str,
    /// Maintainer-facing reason the table exists.
    pub purpose: &'static str,
}

/// Internal catalog tables installed by the extension.
pub const REQUIRED_CATALOG_TABLES: &[CatalogTableSpec] = &[
    CatalogTableSpec {
        name: "koldstore.storage",
        purpose: "registered object storage backends and path templates",
    },
    CatalogTableSpec {
        name: "koldstore.schemas",
        purpose: "managed-table schema versions and initialization state",
    },
    CatalogTableSpec {
        name: "koldstore.manifest",
        purpose: "published cold manifest location and sync state",
    },
    CatalogTableSpec {
        name: "koldstore.jobs",
        purpose: "flush and migration job rows with uniqueness guards",
    },
    CatalogTableSpec {
        name: "koldstore.table_cancel_requests",
        purpose: "cross-session cancel signals that avoid jobs-row lock contention",
    },
    CatalogTableSpec {
        name: "koldstore.cold_segments",
        purpose: "cold object segment catalog for active and retained data",
    },
    CatalogTableSpec {
        name: "koldstore.cold_segment_stats",
        purpose: "normalized per-column segment statistics for predicate pruning",
    },
];

/// Returns required catalog tables that are missing from a parsed bootstrap plan.
#[must_use]
pub fn missing_catalog_tables(plan: &BootstrapPlan) -> Vec<&'static CatalogTableSpec> {
    REQUIRED_CATALOG_TABLES
        .iter()
        .filter(|table| !plan.contains_object(BootstrapObjectKind::Table, table.name))
        .collect()
}
