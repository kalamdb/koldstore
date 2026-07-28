//! Schema-evolution policy for managed table registry refreshes.
//!
//! This module owns pure decisions about which PostgreSQL `ALTER TABLE`
//! outcomes can be represented by a new `koldstore.schemas` version. It does
//! not read PostgreSQL catalogs or write metadata; the extension crate adapts
//! catalog rows into these zero-copy shapes and persists accepted refreshes.
//!
//! Column identity is compared by [`ColumnId`] (`pg_attribute.attnum`), not by
//! name. Renames refresh the schema version while preserving IDs.

use thiserror::Error;

use koldstore_common::ColumnId;

use crate::{PgType, SchemaColumn};

/// Borrowed shape of a column from the current PostgreSQL catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogColumnShape<'a> {
    /// Stable logical ID (`pg_attribute.attnum`).
    pub column_id: ColumnId,
    /// Column name as stored in PostgreSQL for this schema version.
    pub name: &'a str,
    /// Parsed KoldStore type.
    pub pg_type: PgType,
    /// Original PostgreSQL catalog type spelling used for diagnostics.
    pub catalog_type_name: &'a str,
}

/// Inputs required to decide whether a managed table schema can be refreshed.
#[derive(Debug, Clone, Copy)]
pub struct SchemaEvolutionInput<'a> {
    /// Primary-key column IDs recorded in the active schema version (order matters).
    pub active_primary_key: &'a [ColumnId],
    /// Columns recorded in the active schema version.
    pub active_columns: &'a [SchemaColumn],
    /// Indexed column IDs recorded in the active schema version.
    pub active_indexed_columns: &'a [ColumnId],
    /// Primary-key column IDs currently reported by PostgreSQL (order matters).
    pub current_primary_key: &'a [ColumnId],
    /// Columns currently reported by PostgreSQL.
    pub current_columns: &'a [CatalogColumnShape<'a>],
    /// Indexed column IDs currently reported by PostgreSQL.
    pub current_indexed_columns: &'a [ColumnId],
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
    #[error(
        "ALTER TABLE dropped primary-key column_id `{column_id}` from a managed KoldStore table"
    )]
    PrimaryKeyColumnDropped {
        /// Dropped primary-key column ID.
        column_id: ColumnId,
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
        "ALTER TABLE changed type of managed KoldStore column_id `{column_id}` from `{old_type}` to `{new_type}`; type changes require unmanage/manage"
    )]
    ColumnTypeChanged {
        /// Column ID whose type changed.
        column_id: ColumnId,
        /// Type recorded in the active schema.
        old_type: String,
        /// Current PostgreSQL catalog type.
        new_type: String,
    },
}

/// Plans whether an active schema version should be refreshed.
///
/// Supported changes are renames (same ID, new name), additive columns, dropped
/// non-primary-key columns, and index-set changes. Primary-key ID/order changes,
/// unsupported newly visible types, and type changes for the same column ID are
/// rejected because existing cold segments cannot be safely interpreted under
/// the new shape.
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
        let is_pk = input
            .active_primary_key
            .contains(&active_column.column_id);
        let current = input
            .current_columns
            .iter()
            .find(|column| column.column_id == active_column.column_id);

        if is_pk && current.is_none() {
            return Err(SchemaEvolutionError::PrimaryKeyColumnDropped {
                column_id: active_column.column_id,
            });
        }

        if let Some(current) = current {
            if current.catalog_type_name != active_column.catalog_type_name() {
                return Err(SchemaEvolutionError::ColumnTypeChanged {
                    column_id: active_column.column_id,
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
            active.column_id == current.column_id
                && active.name == current.name
                && active.catalog_type_name() == current.catalog_type_name
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(attnum: i16) -> ColumnId {
        ColumnId::from_attnum(attnum)
    }

    fn active_columns() -> Vec<SchemaColumn> {
        vec![
            SchemaColumn::app(1, "id", "int8", false),
            SchemaColumn::app(2, "title", "text", true),
        ]
    }

    fn current_columns<'a>(
        columns: &'a [(i16, &'a str, PgType, &'a str)],
    ) -> Vec<CatalogColumnShape<'a>> {
        columns
            .iter()
            .map(
                |(column_id, name, pg_type, catalog_type_name)| CatalogColumnShape {
                    column_id: ColumnId::from_attnum(*column_id),
                    name,
                    pg_type: *pg_type,
                    catalog_type_name,
                },
            )
            .collect()
    }

    #[test]
    fn unchanged_schema_does_not_refresh() {
        let active_primary_key = vec![id(1)];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "title", PgType::Text, "text"),
        ]);
        let indexed_columns = vec![id(2)];

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
    fn rename_preserves_id_and_refreshes() {
        let active_primary_key = vec![id(1)];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "headline", PgType::Text, "text"),
        ]);

        let action = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect("rename is safe");

        assert_eq!(action, SchemaEvolutionAction::Refresh);
    }

    #[test]
    fn drop_and_add_same_name_uses_new_id_and_refreshes() {
        let active_primary_key = vec![id(1)];
        let active_columns = active_columns();
        // Dropped column_id 2 ("title"); new column reuses the name with attnum 3.
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (3, "title", PgType::Text, "text"),
        ]);

        let action = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect("drop+add same name is a new column identity");

        assert_eq!(action, SchemaEvolutionAction::Refresh);
    }

    #[test]
    fn supported_added_column_refreshes() {
        let active_primary_key = vec![id(1)];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "title", PgType::Text, "text"),
            (3, "note", PgType::Text, "text"),
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
        let active_primary_key = vec![id(1)];
        let current_primary_key = vec![id(2)];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "title", PgType::Text, "text"),
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
    fn primary_key_rename_preserves_identity() {
        let active_primary_key = vec![id(1)];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "message_id", PgType::Int8, "int8"),
            (2, "title", PgType::Text, "text"),
        ]);

        let action = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect("PK rename preserves ID");

        assert_eq!(action, SchemaEvolutionAction::Refresh);
    }

    #[test]
    fn existing_column_type_change_is_rejected() {
        let active_primary_key = vec![id(1)];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "title", PgType::Jsonb, "jsonb"),
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
                column_id: id(2),
                old_type: "text".to_string(),
                new_type: "jsonb".to_string(),
            }
        );
    }

    #[test]
    fn supported_bytea_added_column_refreshes() {
        let active_primary_key = vec![id(1)];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "title", PgType::Text, "text"),
            (3, "raw", PgType::Bytea, "bytea"),
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
