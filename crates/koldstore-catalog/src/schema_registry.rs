//! Catalog-owned schema registry models.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use koldstore_common::{ColumnId, Diagnostic, KoldstoreError};
use koldstore_schema::{PgType, SchemaError};

fn default_active() -> bool {
    true
}

/// One logical column in a versioned catalog schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaColumn {
    /// Stable, never-reused column identity.
    pub column_id: ColumnId,
    /// Logical column name.
    pub name: String,
    /// Parsed PostgreSQL type.
    pub pg_type: PgType,
    /// Original catalog type spelling preserved for diagnostics.
    pub catalog_type_name: String,
    /// Whether the column accepts null values.
    pub nullable: bool,
    /// Whether KoldStore owns this metadata column.
    pub system: bool,
    /// Whether the column is present in this schema version.
    pub active: bool,
    /// PostgreSQL attribute number when known.
    pub attnum: Option<i16>,
    /// One-based logical display order in this schema version.
    pub ordinal: u32,
    /// Frozen backfill default for older cold files missing this column.
    pub initial_default: Option<String>,
    /// Default applied to new inserts (may change after ADD).
    pub insert_default: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SchemaColumnWire {
    column_id: ColumnId,
    name: String,
    type_name: String,
    nullable: bool,
    #[serde(default)]
    system: bool,
    #[serde(default = "default_active")]
    active: bool,
    #[serde(default)]
    attnum: Option<i16>,
    #[serde(default)]
    ordinal: u32,
    #[serde(default)]
    initial_default: Option<String>,
    #[serde(default)]
    insert_default: Option<String>,
}

impl TryFrom<SchemaColumnWire> for SchemaColumn {
    type Error = SchemaError;

    fn try_from(wire: SchemaColumnWire) -> Result<Self, Self::Error> {
        Ok(Self {
            column_id: wire.column_id,
            name: wire.name,
            pg_type: PgType::from_postgres_name(&wire.type_name)?,
            catalog_type_name: wire.type_name,
            nullable: wire.nullable,
            system: wire.system,
            active: wire.active,
            attnum: wire.attnum,
            ordinal: wire.ordinal,
            initial_default: wire.initial_default,
            insert_default: wire.insert_default,
        })
    }
}

impl Serialize for SchemaColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SchemaColumnWire {
            column_id: self.column_id,
            name: self.name.clone(),
            type_name: self.catalog_type_name.clone(),
            nullable: self.nullable,
            system: self.system,
            active: self.active,
            attnum: self.attnum,
            ordinal: self.ordinal,
            initial_default: self.initial_default.clone(),
            insert_default: self.insert_default.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SchemaColumn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        SchemaColumnWire::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

impl SchemaColumn {
    /// Creates an active application column from a supported catalog type.
    ///
    /// # Panics
    ///
    /// Panics when `type_name` is outside the supported type matrix.
    #[must_use]
    pub fn app(
        column_id: ColumnId,
        name: impl Into<String>,
        type_name: impl Into<String>,
        nullable: bool,
    ) -> Self {
        let catalog_type_name = type_name.into();
        let pg_type = PgType::from_postgres_name(&catalog_type_name)
            .expect("schema column builders must use supported PostgreSQL types");
        Self::typed(column_id, name, pg_type, catalog_type_name, nullable, false)
    }

    /// Creates a column from a supported PostgreSQL type.
    #[must_use]
    pub fn typed(
        column_id: ColumnId,
        name: impl Into<String>,
        pg_type: PgType,
        catalog_type_name: impl Into<String>,
        nullable: bool,
        system: bool,
    ) -> Self {
        Self {
            column_id,
            name: name.into(),
            pg_type,
            catalog_type_name: catalog_type_name.into(),
            nullable,
            system,
            active: true,
            attnum: None,
            ordinal: u32::try_from(column_id.get()).unwrap_or(u32::MAX),
            initial_default: None,
            insert_default: None,
        }
    }

    /// Returns the original catalog type spelling.
    #[must_use]
    pub fn catalog_type_name(&self) -> &str {
        &self.catalog_type_name
    }
}

/// One durable version of a managed table schema.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SchemaVersion {
    /// Catalog row identifier.
    pub id: Uuid,
    /// Managed PostgreSQL table OID.
    pub table_oid: u32,
    /// Monotonic schema version.
    pub version: u32,
    /// Logical columns in this version.
    pub columns: Vec<SchemaColumn>,
    /// First unallocated column identifier.
    pub next_column_id: ColumnId,
    /// Whether this is the table's active version.
    pub active: bool,
}

#[derive(Deserialize)]
struct SchemaVersionWire {
    id: Uuid,
    table_oid: u32,
    version: u32,
    columns: Vec<SchemaColumn>,
    next_column_id: ColumnId,
    #[serde(default = "default_active")]
    active: bool,
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut wire = SchemaVersionWire::deserialize(deserializer)?;
        for (position, column) in wire.columns.iter_mut().enumerate() {
            if column.ordinal == 0 {
                column.ordinal = u32::try_from(position + 1).map_err(serde::de::Error::custom)?;
            }
        }
        Ok(Self {
            id: wire.id,
            table_oid: wire.table_oid,
            version: wire.version,
            columns: wire.columns,
            next_column_id: wire.next_column_id,
            active: wire.active,
        })
    }
}

impl SchemaVersion {
    /// Returns active application-owned columns.
    #[must_use]
    pub fn application_columns(&self) -> Vec<&SchemaColumn> {
        self.columns
            .iter()
            .filter(|column| !column.system && column.active)
            .collect()
    }

    /// Returns KoldStore-owned metadata columns.
    #[must_use]
    pub fn system_columns(&self) -> Vec<&SchemaColumn> {
        self.columns.iter().filter(|column| column.system).collect()
    }

    /// Validates primary-key and column-id invariants.
    ///
    /// # Errors
    ///
    /// Returns a catalog validation error for missing primary keys, duplicate
    /// column ids, or a next id that does not advance beyond allocated ids.
    pub fn validate(&self, primary_key: &[&str]) -> koldstore_common::Result<()> {
        if primary_key.is_empty() {
            return Err(catalog_error(
                "missing_primary_key",
                "managed tables require a primary key",
            ));
        }
        for pk_column in primary_key {
            if !self
                .columns
                .iter()
                .any(|column| column.active && column.name == *pk_column)
            {
                return Err(catalog_error(
                    "missing_primary_key_column",
                    format!("primary key column not present in active schema: {pk_column}"),
                ));
            }
        }
        for (index, column) in self.columns.iter().enumerate() {
            if self.columns[..index]
                .iter()
                .any(|other| other.column_id == column.column_id)
            {
                return Err(catalog_error(
                    "duplicate_column_id",
                    format!("column id {} appears more than once", column.column_id),
                ));
            }
            if column.column_id >= self.next_column_id {
                return Err(catalog_error(
                    "invalid_next_column_id",
                    "next_column_id must exceed every allocated column id",
                ));
            }
        }
        Ok(())
    }
}

fn catalog_error(code: &'static str, detail: impl Into<String>) -> KoldstoreError {
    KoldstoreError::CatalogValidation {
        diagnostic: Diagnostic::new(code, detail),
    }
}

/// Compatibility name for callers that still use registry terminology.
pub type SchemaRegistryEntry = SchemaVersion;
