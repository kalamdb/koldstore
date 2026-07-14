//! Schema-evolution policy for managed table registry refreshes.
//!
//! This module owns pure decisions about which PostgreSQL `ALTER TABLE`
//! outcomes can be represented by a new `koldstore.schemas` version. It does
//! not read PostgreSQL catalogs or write metadata; the extension crate adapts
//! catalog rows into these zero-copy shapes and persists accepted refreshes.

use thiserror::Error;

use crate::{PgType, SchemaColumn};

/// Borrowed shape of a column from the current PostgreSQL catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogColumnShape<'a> {
    /// Column name as stored in PostgreSQL.
    pub name: &'a str,
    /// Parsed KoldStore type.
    pub pg_type: PgType,
    /// Original PostgreSQL catalog type spelling used for diagnostics.
    pub catalog_type_name: &'a str,
}

/// Inputs required to decide whether a managed table schema can be refreshed.
#[derive(Debug, Clone, Copy)]
pub struct SchemaEvolutionInput<'a> {
    /// Primary-key columns recorded in the active schema version.
    pub active_primary_key: &'a [String],
    /// Columns recorded in the active schema version.
    pub active_columns: &'a [SchemaColumn],
    /// Indexed columns recorded in the active schema version.
    pub active_indexed_columns: &'a [String],
    /// Primary-key columns currently reported by PostgreSQL.
    pub current_primary_key: &'a [String],
    /// Columns currently reported by PostgreSQL.
    pub current_columns: &'a [CatalogColumnShape<'a>],
    /// Indexed columns currently reported by PostgreSQL.
    pub current_indexed_columns: &'a [String],
}

/// Planned schema-evolution action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaEvolutionAction {
    /// Active registry metadata already matches PostgreSQL.
    Unchanged,
    /// PostgreSQL changed only in ways that can be represented by a new schema
    /// registry version.
    Refresh,
}

/// Unsafe schema-evolution outcome.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaEvolutionError {
    /// PostgreSQL primary-key shape changed after the table became managed.
    #[error(
        "ALTER TABLE changed the primary key of a managed KoldStore table; unmanage and manage the table again"
    )]
    PrimaryKeyChanged,
    /// A primary-key column from the active schema is no longer present.
    #[error("ALTER TABLE dropped primary-key column `{column}` from a managed KoldStore table")]
    PrimaryKeyColumnDropped {
        /// Dropped primary-key column name.
        column: String,
    },
    /// A current catalog column has no MVP cold-storage representation.
    #[error(
        "ALTER TABLE added unsupported type `{type_name}` for managed KoldStore column `{column}`"
    )]
    UnsupportedColumnType {
        /// Column with the unsupported type.
        column: String,
        /// PostgreSQL catalog type spelling.
        type_name: String,
    },
    /// A previously managed column changed type in-place.
    #[error(
        "ALTER TABLE changed type of managed KoldStore column `{column}` from `{old_type}` to `{new_type}`; type changes require unmanage/manage"
    )]
    ColumnTypeChanged {
        /// Column whose type changed.
        column: String,
        /// Type recorded in the active schema.
        old_type: String,
        /// Current PostgreSQL catalog type.
        new_type: String,
    },
}

/// Plans whether an active schema version should be refreshed.
///
/// Supported changes are additive columns, dropped non-primary-key columns, and
/// index-set changes. Primary-key changes, unsupported newly visible types, and
/// type changes for existing managed columns are rejected because existing cold
/// segments cannot be safely interpreted under the new shape.
///
/// # Errors
///
/// Returns [`SchemaEvolutionError`] when PostgreSQL changed the table in a way
/// that cannot be represented by a compatible schema version.
pub fn plan_schema_evolution(
    input: &SchemaEvolutionInput<'_>,
) -> Result<SchemaEvolutionAction, SchemaEvolutionError> {
    if input.active_primary_key != input.current_primary_key {
        return Err(SchemaEvolutionError::PrimaryKeyChanged);
    }

    for current in input.current_columns {
        if !current.pg_type.is_mvp_supported() {
            return Err(SchemaEvolutionError::UnsupportedColumnType {
                column: current.name.to_string(),
                type_name: current.catalog_type_name.to_string(),
            });
        }
    }

    for active_column in input.active_columns {
        if input
            .active_primary_key
            .iter()
            .any(|pk| pk == &active_column.name)
            && !input
                .current_columns
                .iter()
                .any(|column| column.name == active_column.name)
        {
            return Err(SchemaEvolutionError::PrimaryKeyColumnDropped {
                column: active_column.name.clone(),
            });
        }

        if let Some(current) = input
            .current_columns
            .iter()
            .find(|column| column.name == active_column.name)
        {
            if current.catalog_type_name != active_column.catalog_type_name() {
                return Err(SchemaEvolutionError::ColumnTypeChanged {
                    column: active_column.name.clone(),
                    old_type: active_column.catalog_type_name().to_string(),
                    new_type: current.catalog_type_name.to_string(),
                });
            }
        }
    }

    if schema_columns_match(input.active_columns, input.current_columns)
        && input.active_indexed_columns == input.current_indexed_columns
    {
        Ok(SchemaEvolutionAction::Unchanged)
    } else {
        Ok(SchemaEvolutionAction::Refresh)
    }
}

fn schema_columns_match(active: &[SchemaColumn], current: &[CatalogColumnShape<'_>]) -> bool {
    active.len() == current.len()
        && active.iter().zip(current.iter()).all(|(active, current)| {
            active.name == current.name && active.catalog_type_name() == current.catalog_type_name
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active_columns() -> Vec<SchemaColumn> {
        vec![
            SchemaColumn::app("id", "int8", false),
            SchemaColumn::app("title", "text", true),
        ]
    }

    fn current_columns<'a>(
        columns: &'a [(&'a str, PgType, &'a str)],
    ) -> Vec<CatalogColumnShape<'a>> {
        columns
            .iter()
            .map(|(name, pg_type, catalog_type_name)| CatalogColumnShape {
                name,
                pg_type: *pg_type,
                catalog_type_name,
            })
            .collect()
    }

    #[test]
    fn unchanged_schema_does_not_refresh() {
        let active_primary_key = vec!["id".to_string()];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            ("id", PgType::Int8, "int8"),
            ("title", PgType::Text, "text"),
        ]);
        let indexed_columns = vec!["title".to_string()];

        let action = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &indexed_columns,
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &indexed_columns,
        })
        .expect("schema is safe");

        assert_eq!(action, SchemaEvolutionAction::Unchanged);
    }

    #[test]
    fn supported_added_column_refreshes() {
        let active_primary_key = vec!["id".to_string()];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            ("id", PgType::Int8, "int8"),
            ("title", PgType::Text, "text"),
            ("note", PgType::Text, "text"),
        ]);

        let action = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect("schema is safe");

        assert_eq!(action, SchemaEvolutionAction::Refresh);
    }

    #[test]
    fn primary_key_change_is_rejected() {
        let active_primary_key = vec!["id".to_string()];
        let current_primary_key = vec!["title".to_string()];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            ("id", PgType::Int8, "int8"),
            ("title", PgType::Text, "text"),
        ]);

        let error = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &current_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect_err("primary key changes are unsafe");

        assert_eq!(error, SchemaEvolutionError::PrimaryKeyChanged);
    }

    #[test]
    fn existing_column_type_change_is_rejected() {
        let active_primary_key = vec!["id".to_string()];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            ("id", PgType::Int8, "int8"),
            ("title", PgType::Jsonb, "jsonb"),
        ]);

        let error = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect_err("type changes are unsafe");

        assert_eq!(
            error,
            SchemaEvolutionError::ColumnTypeChanged {
                column: "title".to_string(),
                old_type: "text".to_string(),
                new_type: "jsonb".to_string(),
            }
        );
    }

    #[test]
    fn supported_bytea_added_column_refreshes() {
        let active_primary_key = vec!["id".to_string()];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            ("id", PgType::Int8, "int8"),
            ("title", PgType::Text, "text"),
            ("raw", PgType::Bytea, "bytea"),
        ]);

        let action = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect("bytea is an MVP-supported additive column");

        assert_eq!(action, SchemaEvolutionAction::Refresh);
    }
}
