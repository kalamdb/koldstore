//! Mirror relation naming.

//! Mirror relation naming.

use koldstore_common::{is_safe_identifier, TableName};

use super::error::{MirrorError, MirrorResult};

pub use koldstore_common::KOLDSTORE_SCHEMA;
/// Suffix appended to the schema-qualified source identity for its mirror.
pub const CHANGE_LOG_MIRROR_SUFFIX: &str = "__cl";
const MAX_POSTGRES_IDENTIFIER_BYTES: usize = 63;
const MIRROR_NAME_HASH_HEX_LEN: usize = 16;

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
    let mirror_name = mirror_relation_name(source_table);
    if !is_safe_identifier(&mirror_name) {
        return Err(MirrorError::InvalidMirrorName(mirror_name));
    }
    let table_name = TableName::parse(format!("{KOLDSTORE_SCHEMA}.{mirror_name}"))
        .map_err(|_| MirrorError::InvalidMirrorName(mirror_name))?;
    Ok(MirrorRelation::new(table_name))
}

fn mirror_relation_name(source_table: &TableName) -> String {
    let source_name = source_table.schema().map_or_else(
        || source_table.relation().to_string(),
        |schema| format!("{schema}_{}", source_table.relation()),
    );
    bounded_identifier(&source_name, CHANGE_LOG_MIRROR_SUFFIX)
}

/// Builds a deterministic PostgreSQL identifier from a prefix and suffix.
///
/// Long names retain the suffix and replace the omitted middle with a stable
/// hash so independently generated artifacts cannot collide by truncation.
pub(crate) fn bounded_identifier(prefix: &str, suffix: &str) -> String {
    let candidate = format!("{prefix}{suffix}");
    if candidate.len() <= MAX_POSTGRES_IDENTIFIER_BYTES {
        return candidate;
    }
    let prefix_len = MAX_POSTGRES_IDENTIFIER_BYTES - 1 - MIRROR_NAME_HASH_HEX_LEN - suffix.len();
    let hash = stable_name_hash(prefix);
    format!("{}_{hash:016x}{suffix}", &prefix[..prefix_len])
}

/// Returns PostgreSQL's historical first-63-byte truncation for legacy names.
pub(crate) fn legacy_truncated_identifier(prefix: &str, suffix: &str) -> String {
    format!("{prefix}{suffix}")
        .chars()
        .take(MAX_POSTGRES_IDENTIFIER_BYTES)
        .collect()
}

fn stable_name_hash(value: &str) -> u64 {
    value
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn includes_the_source_schema_in_the_mirror_name() {
        let source = TableName::parse("db1.messages").expect("source table name");

        let mirror = mirror_relation_for_source(&source).expect("mirror relation");

        assert_eq!(mirror.table_name().as_str(), "koldstore.db1_messages__cl");
    }

    #[test]
    fn bounds_long_source_names_without_losing_determinism() {
        let source = TableName::parse(format!("{}.{}", "a".repeat(63), "b".repeat(63)))
            .expect("source table name");

        let first = mirror_relation_for_source(&source).expect("first mirror relation");
        let second = mirror_relation_for_source(&source).expect("second mirror relation");

        assert_eq!(first, second);
        assert!(first.relation().len() <= 63);
        assert!(first.relation().ends_with(CHANGE_LOG_MIRROR_SUFFIX));
    }
}
