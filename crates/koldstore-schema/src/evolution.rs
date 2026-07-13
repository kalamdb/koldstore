//! Schema-evolution policy for managed table registry refreshes.
//!
//! Correlates PostgreSQL `attnum` to durable `ColumnId` so rename keeps identity,
//! drop marks columns inactive without reuse, and compatible type promotions are
//! accepted. Name-only matching is not used.

use koldstore_common::ColumnId;
use thiserror::Error;

use crate::PgType;

/// Borrowed active-schema fields needed by evolution policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveColumnShape<'a> {
    /// Stable catalog column identity.
    pub column_id: ColumnId,
    /// PostgreSQL attribute number used only for rename vs drop+add correlation.
    pub attnum: i16,
    /// Logical column name.
    pub name: &'a str,
    /// Parsed type recorded in the active schema version.
    pub pg_type: PgType,
    /// Original PostgreSQL catalog type spelling.
    pub catalog_type_name: &'a str,
}

/// Borrowed shape of a column from the current PostgreSQL catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogColumnShape<'a> {
    /// PostgreSQL attribute number.
    pub attnum: i16,
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
    /// Active columns recorded in the active schema version.
    pub active_columns: &'a [ActiveColumnShape<'a>],
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
    /// Active schema is missing attnum correlation required for hard cutover.
    #[error("managed schema column `{column}` is missing attnum correlation")]
    MissingAttnum {
        /// Column missing attnum.
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
    /// A previously managed column changed type incompatibly.
    #[error(
        "ALTER TABLE changed type of managed KoldStore column `{column}` from `{old_type}` to `{new_type}`; incompatible type changes require unmanage/manage"
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
/// Correlation is by `attnum` → `column_id`. Supported outcomes are ADD, RENAME,
/// DROP of non-PK columns, compatible type promotion, and index-set changes.
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

    for active in input.active_columns {
        if active.attnum <= 0 {
            return Err(SchemaEvolutionError::MissingAttnum {
                column: active.name.to_string(),
            });
        }
    }
    for current in input.current_columns {
        if current.attnum <= 0 {
            return Err(SchemaEvolutionError::MissingAttnum {
                column: current.name.to_string(),
            });
        }
        if !current.pg_type.is_mvp_supported() {
            return Err(SchemaEvolutionError::UnsupportedColumnType {
                column: current.name.to_string(),
                type_name: current.catalog_type_name.to_string(),
            });
        }
    }

    for active in input.active_columns {
        let is_pk = input.active_primary_key.iter().any(|pk| pk == active.name);
        let current = input
            .current_columns
            .iter()
            .find(|column| column.attnum == active.attnum);
        match current {
            None if is_pk => {
                return Err(SchemaEvolutionError::PrimaryKeyColumnDropped {
                    column: active.name.to_string(),
                });
            }
            Some(current) if !active.pg_type.is_compatible_with(current.pg_type) => {
                return Err(SchemaEvolutionError::ColumnTypeChanged {
                    column: current.name.to_string(),
                    old_type: active.catalog_type_name.to_string(),
                    new_type: current.catalog_type_name.to_string(),
                });
            }
            Some(_) | None => {}
        }
    }

    if schema_matches(input) {
        Ok(SchemaEvolutionAction::Unchanged)
    } else {
        Ok(SchemaEvolutionAction::Refresh)
    }
}

fn schema_matches(input: &SchemaEvolutionInput<'_>) -> bool {
    if input.active_indexed_columns != input.current_indexed_columns {
        return false;
    }
    if input.active_columns.len() != input.current_columns.len() {
        return false;
    }
    input.active_columns.iter().all(|active| {
        input
            .current_columns
            .iter()
            .find(|current| current.attnum == active.attnum)
            .is_some_and(|current| {
                current.name == active.name && current.catalog_type_name == active.catalog_type_name
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(value: u64) -> ColumnId {
        ColumnId::new(value).unwrap()
    }

    fn active_columns() -> Vec<ActiveColumnShape<'static>> {
        vec![
            ActiveColumnShape {
                column_id: cid(1),
                attnum: 1,
                name: "id",
                pg_type: PgType::Int8,
                catalog_type_name: "int8",
            },
            ActiveColumnShape {
                column_id: cid(2),
                attnum: 2,
                name: "title",
                pg_type: PgType::Text,
                catalog_type_name: "text",
            },
        ]
    }

    fn current_columns<'a>(
        columns: &'a [(i16, &'a str, PgType, &'a str)],
    ) -> Vec<CatalogColumnShape<'a>> {
        columns
            .iter()
            .map(
                |(attnum, name, pg_type, catalog_type_name)| CatalogColumnShape {
                    attnum: *attnum,
                    name,
                    pg_type: *pg_type,
                    catalog_type_name,
                },
            )
            .collect()
    }

    #[test]
    fn unchanged_schema_does_not_refresh() {
        let active_primary_key = vec!["id".to_string()];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "title", PgType::Text, "text"),
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
    fn rename_by_attnum_refreshes_without_new_identity() {
        let active_primary_key = vec!["id".to_string()];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "body", PgType::Text, "text"),
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
    fn supported_added_column_refreshes() {
        let active_primary_key = vec!["id".to_string()];
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
    fn drop_non_pk_column_refreshes() {
        let active_primary_key = vec!["id".to_string()];
        let active_columns = active_columns();
        let current_columns = current_columns(&[(1, "id", PgType::Int8, "int8")]);

        let action = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect("drop non-pk is safe");

        assert_eq!(action, SchemaEvolutionAction::Refresh);
    }

    #[test]
    fn compatible_int_promotion_refreshes() {
        let active_primary_key = vec!["id".to_string()];
        let active_columns = vec![
            ActiveColumnShape {
                column_id: cid(1),
                attnum: 1,
                name: "id",
                pg_type: PgType::Int8,
                catalog_type_name: "int8",
            },
            ActiveColumnShape {
                column_id: cid(2),
                attnum: 2,
                name: "qty",
                pg_type: PgType::Int4,
                catalog_type_name: "int4",
            },
        ];
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "qty", PgType::Int8, "int8"),
        ]);

        let action = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect("compatible promotion is safe");

        assert_eq!(action, SchemaEvolutionAction::Refresh);
    }

    #[test]
    fn primary_key_change_is_rejected() {
        let active_primary_key = vec!["id".to_string()];
        let current_primary_key = vec!["title".to_string()];
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
    fn incompatible_type_change_is_rejected() {
        let active_primary_key = vec!["id".to_string()];
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
                column: "title".to_string(),
                old_type: "text".to_string(),
                new_type: "jsonb".to_string(),
            }
        );
    }

    #[test]
    fn unsupported_current_type_is_rejected() {
        let active_primary_key = vec!["id".to_string()];
        let active_columns = active_columns();
        let current_columns = current_columns(&[
            (1, "id", PgType::Int8, "int8"),
            (2, "title", PgType::Text, "text"),
            (3, "raw", PgType::Bytea, "bytea"),
        ]);

        let error = plan_schema_evolution(&SchemaEvolutionInput {
            active_primary_key: &active_primary_key,
            active_columns: &active_columns,
            active_indexed_columns: &[],
            current_primary_key: &active_primary_key,
            current_columns: &current_columns,
            current_indexed_columns: &[],
        })
        .expect_err("unsupported types are unsafe");

        assert_eq!(
            error,
            SchemaEvolutionError::UnsupportedColumnType {
                column: "raw".to_string(),
                type_name: "bytea".to_string(),
            }
        );
    }
}
