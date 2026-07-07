//! Existing-table migration ordering decisions.

use koldstore_common::is_safe_identifier;
use koldstore_schema::{PgType, SchemaError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Catalog column metadata needed to choose a safe backfill order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogColumn {
    /// Column name.
    pub name: String,
    /// Supported PostgreSQL type parsed from catalog metadata.
    pub pg_type: PgType,
    /// Original `format_type` spelling preserved for SQL casts.
    pub catalog_type_name: String,
    /// Whether the column participates in the primary key.
    pub is_primary_key: bool,
    /// Whether PostgreSQL marks the column as identity/generated.
    pub identity: bool,
    /// Default expression, when catalog metadata exposes one.
    pub default_expr: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CatalogColumnWire {
    name: String,
    type_name: String,
    is_primary_key: bool,
    identity: bool,
    #[serde(default)]
    default_expr: Option<String>,
}

impl TryFrom<CatalogColumnWire> for CatalogColumn {
    type Error = SchemaError;

    fn try_from(wire: CatalogColumnWire) -> Result<Self, Self::Error> {
        Ok(Self {
            name: wire.name,
            pg_type: PgType::from_postgres_name(&wire.type_name)?,
            catalog_type_name: wire.type_name,
            is_primary_key: wire.is_primary_key,
            identity: wire.identity,
            default_expr: wire.default_expr,
        })
    }
}

impl Serialize for CatalogColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        CatalogColumnWire {
            name: self.name.clone(),
            type_name: self.catalog_type_name.clone(),
            is_primary_key: self.is_primary_key,
            identity: self.identity,
            default_expr: self.default_expr.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CatalogColumn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        CatalogColumnWire::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

impl CatalogColumn {
    /// Creates bigint column metadata.
    #[must_use]
    pub fn bigint(name: impl Into<String>) -> Self {
        Self::typed(name, PgType::Int8, "bigint")
    }

    /// Creates text column metadata.
    #[must_use]
    pub fn text(name: impl Into<String>) -> Self {
        Self::typed(name, PgType::Text, "text")
    }

    /// Creates uuid column metadata.
    #[must_use]
    pub fn uuid(name: impl Into<String>) -> Self {
        Self::typed(name, PgType::Uuid, "uuid")
    }

    /// Creates timestamp column metadata.
    #[must_use]
    pub fn timestamp(name: impl Into<String>) -> Self {
        Self::typed(name, PgType::Timestamptz, "timestamp without time zone")
    }

    /// Creates jsonb column metadata.
    #[must_use]
    pub fn jsonb(name: impl Into<String>) -> Self {
        Self::typed(name, PgType::Jsonb, "jsonb")
    }

    /// Creates column metadata from a supported PostgreSQL type.
    #[must_use]
    pub fn typed(
        name: impl Into<String>,
        pg_type: PgType,
        catalog_type_name: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            pg_type,
            catalog_type_name: catalog_type_name.into(),
            is_primary_key: false,
            identity: false,
            default_expr: None,
        }
    }

    /// Creates column metadata from a raw PostgreSQL catalog type name.
    ///
    /// # Panics
    ///
    /// Panics when `type_name` is outside the MVP support matrix. Tests and
    /// builders should prefer [`Self::typed`] or the typed constructors.
    #[must_use]
    pub fn new(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        let catalog_type_name = type_name.into();
        let pg_type = PgType::from_postgres_name(&catalog_type_name)
            .expect("catalog column builders must use supported PostgreSQL types");
        Self::typed(name, pg_type, catalog_type_name)
    }

    /// Returns the original catalog type spelling for SQL casts.
    #[must_use]
    pub fn catalog_type_name(&self) -> &str {
        &self.catalog_type_name
    }

    /// Marks the column as a primary-key column.
    #[must_use]
    pub fn primary_key(mut self) -> Self {
        self.is_primary_key = true;
        self
    }

    /// Marks the column as PostgreSQL identity/generated.
    #[must_use]
    pub fn identity(mut self) -> Self {
        self.identity = true;
        self
    }

    /// Attaches a default expression from catalog metadata.
    #[must_use]
    pub fn default_expr(mut self, default_expr: impl Into<String>) -> Self {
        self.default_expr = Some(default_expr.into());
        self
    }
}

/// Primary-key metadata for a table being migrated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogPrimaryKey {
    /// Primary-key columns in index order.
    pub columns: Vec<String>,
}

impl CatalogPrimaryKey {
    /// Builds a single-column primary key.
    #[must_use]
    pub fn single(column: impl Into<String>) -> Self {
        Self {
            columns: vec![column.into()],
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
        .find(|column| &column.name == pk_column)
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
    if column.pg_type.is_orderable()
        || PgType::is_orderable_catalog_type(&column.catalog_type_name)
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
