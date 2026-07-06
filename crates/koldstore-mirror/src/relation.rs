//! Mirror relation naming.

use koldstore_common::{is_safe_identifier, TableName};

use crate::{MirrorError, MirrorResult};

/// Schema that owns all clean-schema mirror tables.
pub const KOLDSTORE_SCHEMA: &str = "koldstore";
/// Suffix appended to the source table name for its latest-state mirror.
pub const CHANGE_LOG_MIRROR_SUFFIX: &str = "__cl";

/// Validated mirror table relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorRelation {
    table_name: TableName,
}

impl MirrorRelation {
    /// Creates a mirror relation from a validated table name.
    #[must_use]
    pub const fn new(table_name: TableName) -> Self {
        Self { table_name }
    }

    /// Returns the underlying table name.
    #[must_use]
    pub const fn table_name(&self) -> &TableName {
        &self.table_name
    }

    /// Returns the mirror relation component.
    #[must_use]
    pub fn relation(&self) -> &str {
        self.table_name.relation()
    }

    /// Returns a safely quoted SQL relation reference.
    #[must_use]
    pub fn quoted(&self) -> String {
        self.table_name.quoted()
    }
}

/// Computes the default mirror relation for a source table.
///
/// # Errors
///
/// Returns an error when the generated relation would not be a safe PostgreSQL
/// identifier for pg-koldstore-owned DDL.
pub fn mirror_relation_for_source(source_table: &TableName) -> MirrorResult<MirrorRelation> {
    let mirror_name = format!("{}{}", source_table.relation(), CHANGE_LOG_MIRROR_SUFFIX);
    if !is_safe_identifier(&mirror_name) {
        return Err(MirrorError::InvalidMirrorName(mirror_name));
    }
    let table_name = TableName::parse(format!("{KOLDSTORE_SCHEMA}.{mirror_name}"))
        .map_err(|_| MirrorError::InvalidMirrorName(mirror_name))?;
    Ok(MirrorRelation::new(table_name))
}
