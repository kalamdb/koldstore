//! Existing-table migration ordering decisions.

use koldstore_common::{is_safe_identifier, ColumnId, ColumnRef};
use koldstore_schema::PgType;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use koldstore_schema::CatalogColumn;

/// Primary-key metadata for a table being migrated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogPrimaryKey {
    /// Primary-key columns in index order.
    pub columns: Vec<ColumnRef>,
}

impl CatalogPrimaryKey {
    /// Builds a single-column primary key.
    #[must_use]
    pub fn single(column_id: i16, column: impl Into<String>) -> Self {
        Self {
            columns: vec![ColumnRef::new(ColumnId::from_attnum(column_id), column)],
        }
    }
}

/// Source of the selected migration ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingSource {
    /// Single auto-increment primary key.
    AutoIncrementPrimaryKey,
    /// User-provided stable ordering column.
    ExplicitColumn,
}

/// Oldest-to-newest ordering used by async existing-row backfill.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationOrdering {
    /// Column used in `ORDER BY`.
    pub column: String,
    /// How the ordering was selected.
    pub source: OrderingSource,
    /// `true` means `ORDER BY column ASC` maps oldest to newest.
    pub ascending_oldest_first: bool,
}

/// Input required to choose a migration ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationOrderingRequest {
    /// Primary-key metadata.
    pub primary_key: CatalogPrimaryKey,
    /// Table columns.
    pub columns: Vec<CatalogColumn>,
    /// Optional user-provided ordering column.
    pub explicit_order_column: Option<String>,
}

/// Ordering selection error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MigrationOrderingError {
    /// Explicit ordering column is unsafe to quote.
    #[error("invalid migration order column `{0}`")]
    InvalidOrderColumn(String),
    /// Explicit ordering column does not exist.
    #[error("migration order column `{0}` does not exist")]
    MissingExplicitColumn(String),
    /// Column type cannot provide an oldest-to-newest order.
    #[error("migration order column `{column}` has unsupported type `{type_name}`")]
    UnsupportedOrderColumnType { column: String, type_name: String },
    /// Existing rows need an explicit age/order indicator.
    #[error(
        "existing table migration requires an auto-increment primary key or explicit order column"
    )]
    MissingOrderingIndicator,
}

/// Chooses the oldest-to-newest migration ordering.
///
/// # Errors
///
/// Returns an error when neither a single auto-increment primary key nor an
/// explicit orderable column is available.
pub fn choose_migration_ordering(
    request: &MigrationOrderingRequest,
) -> Result<MigrationOrdering, MigrationOrderingError> {
    if let Some(explicit) = request.explicit_order_column.as_deref() {
        let explicit = explicit.trim();
        if !is_safe_identifier(explicit) {
            return Err(MigrationOrderingError::InvalidOrderColumn(
                explicit.to_string(),
            ));
        }
        let column = request
            .columns
            .iter()
            .find(|column| column.name == explicit)
            .ok_or_else(|| MigrationOrderingError::MissingExplicitColumn(explicit.to_string()))?;
        ensure_orderable(column)?;
        return Ok(MigrationOrdering {
            column: column.name.clone(),
            source: OrderingSource::ExplicitColumn,
            ascending_oldest_first: true,
        });
    }

    let [pk_column] = request.primary_key.columns.as_slice() else {
        return Err(MigrationOrderingError::MissingOrderingIndicator);
    };
    let Some(column) = request
        .columns
        .iter()
        .find(|column| column.column_id == pk_column.column_id)
    else {
        return Err(MigrationOrderingError::MissingOrderingIndicator);
    };

    if column.is_primary_key && is_auto_increment_column(column) {
        ensure_orderable(column)?;
        Ok(MigrationOrdering {
            column: column.name.clone(),
            source: OrderingSource::AutoIncrementPrimaryKey,
            ascending_oldest_first: true,
        })
    } else {
        Err(MigrationOrderingError::MissingOrderingIndicator)
    }
}

fn ensure_orderable(column: &CatalogColumn) -> Result<(), MigrationOrderingError> {
    if column.pg_type.is_orderable() || PgType::is_orderable_catalog_type(&column.catalog_type_name)
    {
        Ok(())
    } else {
        Err(MigrationOrderingError::UnsupportedOrderColumnType {
            column: column.name.clone(),
            type_name: column.catalog_type_name.clone(),
        })
    }
}

fn is_auto_increment_column(column: &CatalogColumn) -> bool {
    column.identity
        || column
            .default_expr
            .as_deref()
            .map(|default| default.to_ascii_lowercase().contains("nextval("))
            .unwrap_or(false)
}
