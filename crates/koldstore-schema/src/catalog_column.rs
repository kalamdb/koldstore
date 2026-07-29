//! Runtime table-column metadata shared by migrate, merge-scan, and flush.
//!
//! Lives in `koldstore-schema` (not migrate) so scan/flush can depend on column
//! shapes without pulling migration workflow types.

use koldstore_common::ColumnId;
use serde::{Deserialize, Serialize};

use crate::{PgType, SchemaError};

/// Catalog column metadata for migration ordering and runtime scan/flush.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogColumn {
    /// Stable column ID from `pg_attribute.attnum`.
    pub column_id: ColumnId,
    /// Column name.
    pub name: String,
    /// Supported PostgreSQL type parsed from catalog metadata.
    pub pg_type: PgType,
    /// Original `format_type` spelling preserved for SQL casts.
    pub catalog_type_name: String,
    /// Whether the column participates in the primary key.
    pub is_primary_key: bool,
    /// Whether PostgreSQL permits NULL values.
    pub nullable: bool,
    /// Whether PostgreSQL marks the column as an identity column.
    pub identity: bool,
    /// Whether PostgreSQL computes the column from a generation expression.
    pub generated: bool,
    /// Default expression, when catalog metadata exposes one.
    pub default_expr: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CatalogColumnWire {
    column_id: ColumnId,
    name: String,
    type_name: String,
    is_primary_key: bool,
    #[serde(default = "default_nullable")]
    nullable: bool,
    identity: bool,
    #[serde(default)]
    generated: bool,
    #[serde(default)]
    default_expr: Option<String>,
}

impl TryFrom<CatalogColumnWire> for CatalogColumn {
    type Error = SchemaError;

    fn try_from(wire: CatalogColumnWire) -> Result<Self, Self::Error> {
        Ok(Self {
            column_id: wire.column_id,
            name: wire.name,
            pg_type: PgType::from_postgres_name(&wire.type_name)?,
            catalog_type_name: wire.type_name,
            is_primary_key: wire.is_primary_key,
            nullable: wire.nullable,
            identity: wire.identity,
            generated: wire.generated,
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
            column_id: self.column_id,
            name: self.name.clone(),
            type_name: self.catalog_type_name.clone(),
            is_primary_key: self.is_primary_key,
            nullable: self.nullable,
            identity: self.identity,
            generated: self.generated,
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
    pub fn bigint(column_id: i16, name: impl Into<String>) -> Self {
        Self::typed(column_id, name, PgType::Int8, "bigint")
    }

    /// Creates text column metadata.
    #[must_use]
    pub fn text(column_id: i16, name: impl Into<String>) -> Self {
        Self::typed(column_id, name, PgType::Text, "text")
    }

    /// Creates uuid column metadata.
    #[must_use]
    pub fn uuid(column_id: i16, name: impl Into<String>) -> Self {
        Self::typed(column_id, name, PgType::Uuid, "uuid")
    }

    /// Creates timestamp column metadata.
    #[must_use]
    pub fn timestamp(column_id: i16, name: impl Into<String>) -> Self {
        Self::typed(
            column_id,
            name,
            PgType::Timestamptz,
            "timestamp without time zone",
        )
    }

    /// Creates jsonb column metadata.
    #[must_use]
    pub fn jsonb(column_id: i16, name: impl Into<String>) -> Self {
        Self::typed(column_id, name, PgType::Jsonb, "jsonb")
    }

    /// Creates column metadata from a supported PostgreSQL type.
    #[must_use]
    pub fn typed(
        column_id: i16,
        name: impl Into<String>,
        pg_type: PgType,
        catalog_type_name: impl Into<String>,
    ) -> Self {
        Self {
            column_id: ColumnId::from_attnum(column_id),
            name: name.into(),
            pg_type,
            catalog_type_name: catalog_type_name.into(),
            is_primary_key: false,
            nullable: false,
            identity: false,
            generated: false,
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
    pub fn new(column_id: i16, name: impl Into<String>, type_name: impl Into<String>) -> Self {
        let catalog_type_name = type_name.into();
        let pg_type = PgType::from_postgres_name(&catalog_type_name)
            .expect("catalog column builders must use supported PostgreSQL types");
        Self::typed(column_id, name, pg_type, catalog_type_name)
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

const fn default_nullable() -> bool {
    true
}
