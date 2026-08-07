//! Schema registry insertion helpers.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use koldstore_common::{ColumnId, ColumnRef, ManageTableOptions, SqlParamType, SqlStatement};
use koldstore_common::{
    PgCollation, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
    PrimaryKeyShape, StorageId, TableOid,
};
use koldstore_schema::{normalize_type_name, MirrorInitializationState, SchemaColumn, TypeMatrix};

/// Initial schema version for a managed table.
pub const INITIAL_SCHEMA_VERSION: u32 = 1;
/// Type matrix JSON schema version stored in `koldstore.schemas`.
pub const TYPE_MATRIX_CAPTURE_VERSION: u32 = 1;

const REGISTER_SCHEMA_SQL: &str = r#"
INSERT INTO koldstore.schemas AS s (
    id,
    table_oid,
    version,
    active,
    table_type,
    columns,
    primary_key,
    scope_column,
    mirror_relation,
    primary_key_shape,
    initialization_state,
    indexed_columns,
    type_matrix,
    options,
    storage_id
)
VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6::jsonb,
    $7::jsonb,
    $8,
    $9::text::regclass,
    $10::jsonb,
    $11,
    $12::jsonb,
    $13::jsonb,
    $14::jsonb,
    $15
)
ON CONFLICT (table_oid, version) DO UPDATE
SET active = EXCLUDED.active,
    table_type = EXCLUDED.table_type,
    columns = EXCLUDED.columns,
    primary_key = EXCLUDED.primary_key,
    scope_column = EXCLUDED.scope_column,
    mirror_relation = EXCLUDED.mirror_relation,
    primary_key_shape = EXCLUDED.primary_key_shape,
    initialization_state = EXCLUDED.initialization_state,
    indexed_columns = EXCLUDED.indexed_columns,
    type_matrix = EXCLUDED.type_matrix,
    options = EXCLUDED.options,
    storage_id = EXCLUDED.storage_id,
    updated_at = now()
RETURNING s.id
"#;

/// Schema registry planning result.
pub type RegistryResult<T> = Result<T, RegistryError>;

/// Schema registry validation or planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegistryError {
    /// Table type must be shared or user.
    #[error("unsupported table_type `{0}`")]
    UnsupportedTableType(String),
    /// A registered table must have a stable PostgreSQL oid.
    #[error("table_oid cannot be zero")]
    MissingTableOid,
    /// Storage id is missing.
    #[error("storage_id cannot be nil")]
    MissingStorageId,
    /// Primary key metadata is missing or invalid.
    #[error("primary_key cannot be empty")]
    MissingPrimaryKey,
    /// Change-log mirror relation is missing.
    #[error("mirror_relation cannot be empty")]
    MissingMirrorRelation,
    /// Exact primary-key shape is missing.
    #[error("primary_key_shape cannot be empty")]
    MissingPrimaryKeyShape,
    /// PostgreSQL equality can collapse byte-distinct primary-key values.
    #[error(
        "primary-key column `{column}` uses unsupported nondeterministic collation `{collation}`"
    )]
    NondeterministicPrimaryKeyCollation {
        /// Primary-key column using the collation.
        column: String,
        /// Qualified collation identity, or `default` for the database default.
        collation: String,
    },
    /// User-scoped table metadata is missing its scope column.
    #[error("user-scoped table requires scope_column")]
    MissingScopeColumn,
    /// Operator pruning/Bloom column list references an unknown column.
    #[error("unknown {field} column `{column}`")]
    UnknownColdMetadataColumn {
        /// Option field name (`pruning_columns` or `bloom_filter_columns`).
        field: &'static str,
        /// Operator-supplied column name.
        column: String,
    },
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Cold metadata columns derived from preserved hot indexes and primary keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdMetadataCandidates {
    /// Columns worth recording min/max/null-count style statistics for.
    pub stats_columns: Vec<ColumnRef>,
    /// Columns configured for Parquet bloom filters.
    pub bloom_filter_columns: Vec<ColumnRef>,
}

/// Source of a column's cold metadata eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexedColumnSource {
    /// Column participates in the application primary key.
    PrimaryKey,
    /// Column participates in a UNIQUE index or constraint.
    Unique,
    /// Column participates in a foreign key.
    ForeignKey,
    /// Column participates in a secondary index.
    SecondaryIndex,
}

/// Structured metadata for one indexed/constraint-derived column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedColumnMetadata {
    /// Stable column ID.
    pub column_id: ColumnId,
    /// Column name.
    pub column: String,
    /// Metadata source.
    pub source: IndexedColumnSource,
    /// Optional source index/constraint name.
    pub source_name: Option<String>,
    /// One-based ordinal within the source key.
    pub ordinal: u32,
    /// Whether the source guarantees uniqueness.
    pub unique: bool,
    /// Whether this column is part of the application primary key.
    pub primary_key: bool,
    /// Whether this column is part of a foreign key.
    pub foreign_key: bool,
    /// Whether min/max stats are safe to collect for this column type.
    pub supports_stats: bool,
    /// Whether bloom filters are safe to collect for this column type.
    pub supports_bloom: bool,
}

impl IndexedColumnMetadata {
    /// Creates primary-key column metadata.
    #[must_use]
    pub fn primary_key(column: &ColumnRef, ordinal: u32) -> Self {
        Self {
            column_id: column.column_id,
            column: column.name.clone(),
            source: IndexedColumnSource::PrimaryKey,
            source_name: Some("primary_key".to_string()),
            ordinal,
            unique: true,
            primary_key: true,
            foreign_key: false,
            supports_stats: true,
            supports_bloom: true,
        }
    }

    /// Creates secondary-index column metadata.
    #[must_use]
    pub fn secondary_index(column: &ColumnRef, ordinal: u32) -> Self {
        Self {
            column_id: column.column_id,
            column: column.name.clone(),
            source: IndexedColumnSource::SecondaryIndex,
            source_name: None,
            ordinal,
            unique: false,
            primary_key: false,
            foreign_key: false,
            supports_stats: true,
            supports_bloom: true,
        }
    }
}

/// Ordered index shape retained for future composite pruning/order planning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedIndexMetadata {
    /// Index or constraint name.
    pub name: String,
    /// Columns in index key order.
    pub columns: Vec<ColumnRef>,
    /// Whether the ordered key is unique.
    pub unique: bool,
}

/// Typed cold metadata configuration stored in schema options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdMetadataConfig {
    /// Columns worth recording min/max/null-count style statistics for.
    pub stats_columns: Vec<ColumnRef>,
    /// Columns configured for Parquet bloom filters.
    pub bloom_filter_columns: Vec<ColumnRef>,
    /// Structured metadata for columns selected from indexes/constraints.
    pub indexed_columns: Vec<IndexedColumnMetadata>,
    /// Composite index shapes retained for future ordered pruning.
    pub ordered_indexes: Vec<OrderedIndexMetadata>,
}

impl ColdMetadataConfig {
    /// Returns true when no cold metadata is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stats_columns.is_empty()
            && self.bloom_filter_columns.is_empty()
            && self.indexed_columns.is_empty()
            && self.ordered_indexes.is_empty()
    }
}

/// Metadata recorded for a greenfield registration.
#[derive(Debug, Clone, PartialEq)]
pub struct RegistrationMetadata {
    /// Table oid.
    pub table_oid: TableOid,
    /// Table type.
    pub table_type: String,
    /// Storage id.
    pub storage_id: StorageId,
    /// Scope column.
    pub scope_column: Option<String>,
    /// Table-specific change-log mirror relation.
    pub mirror_relation: Option<String>,
    /// Exact primary-key shape captured from PostgreSQL catalogs.
    pub primary_key_shape: Option<PrimaryKeyShape>,
    /// Mirror initialization lifecycle state.
    pub initialization_state: MirrorInitializationState,
    /// Whether the schema row is active.
    pub active: bool,
    /// Primary key columns.
    pub primary_key: Vec<ColumnRef>,
    /// Application column metadata.
    pub columns: Vec<SchemaColumn>,
    /// Indexed columns used as cold stats/bloom candidates.
    pub indexed_columns: Vec<ColumnRef>,
    /// Captured type support/coercion metadata.
    pub type_matrix: Value,
    /// Additional manage-table options.
    pub options: ManageTableOptions,
}

/// Prepared JSON metadata for `koldstore.schemas`.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedRegistrationMetadata {
    /// Table oid.
    pub table_oid: TableOid,
    /// Schema version.
    pub version: u32,
    /// Whether the schema row is active.
    pub active: bool,
    /// Managed table type.
    pub table_type: String,
    /// Serialized app and system columns.
    pub columns: Value,
    /// Serialized preserved primary key columns.
    pub primary_key: Value,
    /// Effective scope column.
    pub scope_column: Option<String>,
    /// Stored mirror relation identity.
    pub mirror_relation: Option<String>,
    /// Serialized exact primary-key shape.
    pub primary_key_shape: Value,
    /// Stored mirror initialization state.
    pub initialization_state: String,
    /// Serialized indexed column names.
    pub indexed_columns: Value,
    /// Type matrix JSON.
    pub type_matrix: Value,
    /// Options JSON including flush policy.
    pub options: Value,
    /// Storage registration id.
    pub storage_id: StorageId,
}

/// Planned `koldstore.schemas` catalog insertion.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaRegistryPlan {
    /// Schema registry row id to bind as `$1`.
    pub schema_id: Uuid,
    /// Prepared metadata values to bind as `$2` through `$14`.
    pub metadata: PreparedRegistrationMetadata,
    /// Parameterized SPI statement.
    pub statement: SqlStatement,
}

impl RegistrationMetadata {
    /// Returns true when metadata is sufficient to activate a managed table.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.table_oid.get() != 0
            && matches!(self.table_type.as_str(), "shared" | "user")
            && !self.storage_id.as_str().is_empty()
            && !self.primary_key.is_empty()
            && self
                .primary_key
                .iter()
                .all(|column| !column.name.trim().is_empty())
            && self
                .mirror_relation
                .as_deref()
                .map(str::trim)
                .filter(|relation| !relation.is_empty())
                .is_some()
            && self.primary_key_shape.is_some()
            && (self.table_type == "shared"
                || self
                    .scope_column
                    .as_deref()
                    .map(str::trim)
                    .filter(|column| !column.is_empty())
                    .is_some())
    }

    /// Validates greenfield schema registry metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when required migration metadata is missing or invalid.
    pub fn validate(&self) -> RegistryResult<()> {
        if self.table_oid.get() == 0 {
            return Err(RegistryError::MissingTableOid);
        }
        if !matches!(self.table_type.as_str(), "shared" | "user") {
            return Err(RegistryError::UnsupportedTableType(self.table_type.clone()));
        }
        if self.storage_id.as_str().is_empty() {
            return Err(RegistryError::MissingStorageId);
        }
        if self.primary_key.is_empty()
            || self
                .primary_key
                .iter()
                .any(|column| column.name.trim().is_empty())
        {
            return Err(RegistryError::MissingPrimaryKey);
        }
        if self
            .mirror_relation
            .as_deref()
            .map(str::trim)
            .filter(|relation| !relation.is_empty())
            .is_none()
        {
            return Err(RegistryError::MissingMirrorRelation);
        }
        if self.primary_key_shape.is_none() {
            return Err(RegistryError::MissingPrimaryKeyShape);
        }
        if self.table_type == "user"
            && self
                .scope_column
                .as_deref()
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .is_none()
        {
            return Err(RegistryError::MissingScopeColumn);
        }

        Ok(())
    }

    /// Serializes registry metadata into the shape written to `koldstore.schemas`.
    ///
    /// # Errors
    ///
    /// Returns an error when validation fails.
    pub fn prepare(&self) -> RegistryResult<PreparedRegistrationMetadata> {
        self.validate()?;

        let mut options = self.options.to_value();
        // Drop operator list keys from the persisted options object once they are
        // folded into cold_metadata (canonical effective set).
        if let Some(object) = options.as_object_mut() {
            object.remove("pruning_columns");
            object.remove("bloom_filter_columns");
        }
        let cold_metadata = cold_metadata_config_for_registration(
            &self.primary_key,
            &self.indexed_columns,
            &self.columns,
            self.options.pruning_columns.as_deref(),
            self.options.bloom_filter_columns.as_deref(),
        )?;
        if !cold_metadata.is_empty() {
            let object = options_object_mut(&mut options)?;
            object.insert(
                "cold_metadata".to_string(),
                serde_json::to_value(cold_metadata).unwrap_or_else(|_| Value::Object(Map::new())),
            );
        }
        let type_matrix = if self.type_matrix.is_null() {
            capture_type_matrix(&self.columns)
        } else {
            self.type_matrix.clone()
        };

        Ok(PreparedRegistrationMetadata {
            table_oid: self.table_oid,
            version: INITIAL_SCHEMA_VERSION,
            active: self.active,
            table_type: self.table_type.clone(),
            columns: serde_json::to_value(&self.columns).unwrap_or_else(|_| Value::Array(vec![])),
            primary_key: serde_json::json!(self.primary_key),
            scope_column: self
                .scope_column
                .as_deref()
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .map(ToString::to_string),
            mirror_relation: self
                .mirror_relation
                .as_deref()
                .map(str::trim)
                .filter(|relation| !relation.is_empty())
                .map(ToString::to_string),
            primary_key_shape: serde_json::to_value(
                self.primary_key_shape
                    .as_ref()
                    .expect("primary_key_shape validated"),
            )
            .unwrap_or_else(|_| Value::Array(vec![])),
            initialization_state: self.initialization_state.as_str().to_string(),
            indexed_columns: serde_json::json!(self.indexed_columns),
            type_matrix,
            options,
            storage_id: self.storage_id.clone(),
        })
    }
}

/// Converts catalog column metadata into schema registry column records.
#[must_use]
pub fn schema_columns_from_catalog(columns: &[crate::order::CatalogColumn]) -> Vec<SchemaColumn> {
    columns
        .iter()
        .map(|column| {
            SchemaColumn::typed(
                column.column_id.get(),
                column.name.clone(),
                column.pg_type,
                column.catalog_type_name(),
                true,
            )
        })
        .collect()
}

/// Plans activation of a managed schema after mirror initialization completes.
///
/// # Errors
///
/// Returns an error when `table_oid` is zero or statement metadata cannot be
/// prepared.
pub fn plan_activate_managed_schema(table_oid: TableOid) -> RegistryResult<SqlStatement> {
    if table_oid.get() == 0 {
        return Err(RegistryError::MissingTableOid);
    }

    SqlStatement::write(
        "activate managed schema after mirror initialization",
        r#"
UPDATE koldstore.schemas
SET active = true,
    initialization_state = 'complete',
    options = jsonb_set(options, '{migration_status}', '"active"'::jsonb, true),
    updated_at = now()
WHERE table_oid = $1::oid
"#,
    )
    .map_err(|error| RegistryError::Spi(error.to_string()))
}

/// Plans an update of active schema `options` for a managed table.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_update_schema_options() -> RegistryResult<SqlStatement> {
    SqlStatement::write_with_params(
        "update managed schema options",
        "UPDATE koldstore.schemas SET options = $2 WHERE table_oid = $1",
        [SqlParamType::Oid, SqlParamType::Jsonb],
    )
    .map_err(|error| RegistryError::Spi(error.to_string()))
}

/// Plans enabling/disabling automatic flush for an active managed table.
///
/// When `enabled` is true, removes the `auto_flush` opt-out key; otherwise
/// stores `auto_flush = false`.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_set_table_auto_flush() -> RegistryResult<SqlStatement> {
    SqlStatement::write_with_params(
        "set table auto_flush option",
        r#"
WITH updated AS (
    UPDATE koldstore.schemas
    SET options = CASE
            WHEN $2::boolean THEN options - 'auto_flush'
            ELSE jsonb_set(COALESCE(options, '{}'::jsonb), '{auto_flush}', 'false'::jsonb, true)
        END,
        updated_at = now()
    WHERE table_oid = $1::oid
      AND active
    RETURNING 1
)
SELECT EXISTS (SELECT 1 FROM updated)
"#,
        [SqlParamType::Oid, SqlParamType::Boolean],
    )
    .map_err(|error| RegistryError::Spi(error.to_string()))
}

/// Plans an `EXISTS` probe for whether a table currently has any rows.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_table_has_rows(table: &crate::QualifiedTableName) -> RegistryResult<SqlStatement> {
    SqlStatement::read(
        "probe whether table has rows",
        &format!(
            "SELECT EXISTS (SELECT 1 FROM ONLY {} LIMIT 1)",
            table.quoted()
        ),
    )
    .map_err(|error| RegistryError::Spi(error.to_string()))
}

/// Builds a schema registry insert plan with a generated schema id.
///
/// # Errors
///
/// Returns an error when registration metadata is incomplete or statement
/// metadata cannot be prepared.
pub fn plan_schema_registry_insert(
    metadata: &RegistrationMetadata,
) -> RegistryResult<SchemaRegistryPlan> {
    plan_schema_registry_insert_with_id(metadata, Uuid::new_v4())
}

/// Builds a schema registry insert plan with a caller-provided schema id.
///
/// # Errors
///
/// Returns an error when registration metadata is incomplete or statement
/// metadata cannot be prepared.
pub fn plan_schema_registry_insert_with_id(
    metadata: &RegistrationMetadata,
    schema_id: Uuid,
) -> RegistryResult<SchemaRegistryPlan> {
    let metadata = metadata.prepare()?;
    let statement = SqlStatement::write("register managed table schema", REGISTER_SCHEMA_SQL)
        .map_err(|error| RegistryError::Spi(error.to_string()))?;

    Ok(SchemaRegistryPlan {
        schema_id,
        metadata,
        statement,
    })
}

/// Builds a schema registry insert plan from prepared metadata.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_schema_registry_insert_prepared(
    schema_id: Uuid,
    metadata: PreparedRegistrationMetadata,
) -> RegistryResult<SchemaRegistryPlan> {
    let statement = SqlStatement::write("register managed table schema", REGISTER_SCHEMA_SQL)
        .map_err(|error| RegistryError::Spi(error.to_string()))?;

    Ok(SchemaRegistryPlan {
        schema_id,
        metadata,
        statement,
    })
}

/// Primary-key column shape as decoded from PostgreSQL catalog JSON.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PrimaryKeyShapeCatalogRow {
    /// Stable column ID from `pg_attribute.attnum`.
    pub column_id: ColumnId,
    /// Column name.
    pub column: String,
    /// One-based primary-key ordinal.
    pub ordinal: u16,
    /// PostgreSQL type OID.
    pub type_oid: u32,
    /// PostgreSQL type name or rendered base type for domain-backed keys.
    pub type_name: String,
    /// PostgreSQL type modifier.
    pub typmod: i32,
    /// Optional non-default collation identity.
    pub collation: Option<String>,
    /// Whether PostgreSQL requires byte-identical strings for collation equality.
    #[serde(default)]
    pub collation_deterministic: Option<bool>,
    /// Optional domain type identity.
    pub domain_identity: Option<String>,
    /// Whether PostgreSQL marks the column as non-null.
    pub not_null: bool,
}

/// Builds the catalog query that captures exact primary-key shape.
///
/// # Errors
///
/// Returns an error when `table_oid` is zero or statement metadata cannot be
/// represented by the SPI helper.
pub fn primary_key_shape_probe_plan(table_oid: TableOid) -> RegistryResult<SqlStatement> {
    if table_oid.get() == 0 {
        return Err(RegistryError::MissingTableOid);
    }

    SqlStatement::read(
        "capture primary-key shape",
        r#"
SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object(
            'column_id', a.attnum,
            'column', a.attname,
            'ordinal', key_position.ordinality,
            'type_oid', a.atttypid::bigint,
            'type_name', format_type(COALESCE(NULLIF(t.typbasetype, 0), a.atttypid), a.atttypmod),
            'typmod', a.atttypmod,
            'collation', CASE
                WHEN coll.oid IS NULL OR coll.collname = 'default' THEN NULL
                ELSE format('%I.%I', coll_ns.nspname, coll.collname)
            END,
            'collation_deterministic', coll.collisdeterministic,
            'domain_identity', CASE
                WHEN t.typtype = 'd' THEN format('%I.%I', type_ns.nspname, t.typname)
                ELSE NULL
            END,
            'not_null', a.attnotnull
        )
        ORDER BY key_position.ordinality
    )::text,
    '[]'
)
FROM pg_index i
JOIN unnest(i.indkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON true
JOIN pg_attribute a
  ON a.attrelid = i.indrelid
 AND a.attnum = key_position.attnum
JOIN pg_type t
  ON t.oid = a.atttypid
JOIN pg_namespace type_ns
  ON type_ns.oid = t.typnamespace
LEFT JOIN pg_collation coll
  ON coll.oid = a.attcollation
 AND a.attcollation <> 0
LEFT JOIN pg_namespace coll_ns
  ON coll_ns.oid = coll.collnamespace
WHERE i.indrelid = $1::oid
  AND i.indisprimary
  AND i.indexprs IS NULL
"#,
    )
    .map_err(|error| RegistryError::Spi(error.to_string()))
}

/// Converts decoded catalog rows into a type-safe primary-key shape.
///
/// # Errors
///
/// Returns an error when the catalog rows are empty or contain invalid primary
/// key metadata.
pub fn primary_key_shape_from_catalog_rows(
    rows: Vec<PrimaryKeyShapeCatalogRow>,
) -> RegistryResult<PrimaryKeyShape> {
    if let Some(row) = rows
        .iter()
        .find(|row| row.collation_deterministic == Some(false))
    {
        return Err(RegistryError::NondeterministicPrimaryKeyCollation {
            column: row.column.clone(),
            collation: row
                .collation
                .clone()
                .unwrap_or_else(|| "default".to_string()),
        });
    }
    let columns = rows
        .into_iter()
        .map(|row| {
            Ok(PrimaryKeyColumnShape::new(
                row.column_id,
                PkColumn::new(row.column).map_err(|error| RegistryError::Spi(error.to_string()))?,
                PkOrdinal::new(row.ordinal)
                    .map_err(|error| RegistryError::Spi(error.to_string()))?,
                PgTypeOid::new(row.type_oid)
                    .map_err(|error| RegistryError::Spi(error.to_string()))?,
                PgTypeName::new(row.type_name)
                    .map_err(|error| RegistryError::Spi(error.to_string()))?,
                PgTypmod::new(row.typmod),
                row.collation
                    .map(PgCollation::new)
                    .transpose()
                    .map_err(|error| RegistryError::Spi(error.to_string()))?,
                row.domain_identity
                    .map(PgTypeName::new)
                    .transpose()
                    .map_err(|error| RegistryError::Spi(error.to_string()))?,
                row.not_null,
            ))
        })
        .collect::<RegistryResult<Vec<_>>>()?;

    PrimaryKeyShape::new(columns).map_err(|error| RegistryError::Spi(error.to_string()))
}

/// Captures supported-type metadata for the columns being registered.
#[must_use]
pub fn capture_type_matrix(columns: &[SchemaColumn]) -> Value {
    let matrix = TypeMatrix::postgres_15_default();
    let columns = columns
        .iter()
        .map(|column| {
            let type_name = column.catalog_type_name();
            let support = matrix.support_for(&normalize_type_name(type_name));
            match support.diagnostic {
                Some(diagnostic) => serde_json::json!({
                    "name": column.name,
                    "type_name": type_name,
                    "supported": support.supported,
                    "diagnostic": diagnostic,
                }),
                None => serde_json::json!({
                    "name": column.name,
                    "type_name": type_name,
                    "supported": support.supported,
                }),
            }
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "version": TYPE_MATRIX_CAPTURE_VERSION,
        "columns": columns,
    })
}

/// Builds cold stats and bloom candidate metadata from PK and indexed columns.
#[must_use]
pub fn cold_metadata_candidates(
    primary_key: &[ColumnRef],
    indexed_columns: &[ColumnRef],
) -> ColdMetadataCandidates {
    let config = cold_metadata_config(primary_key, indexed_columns);

    ColdMetadataCandidates {
        stats_columns: config.stats_columns,
        bloom_filter_columns: config.bloom_filter_columns,
    }
}

/// Builds typed cold metadata configuration from PK and indexed columns.
#[must_use]
pub fn cold_metadata_config(
    primary_key: &[ColumnRef],
    indexed_columns: &[ColumnRef],
) -> ColdMetadataConfig {
    cold_metadata_config_with_overrides(primary_key, indexed_columns, None, None)
}

/// Builds cold metadata, applying optional operator pruning/Bloom column lists.
///
/// When an operator list is `None`, candidates are auto-derived (indexed for
/// stats; PK ∪ indexed for Bloom). When `Some`, the list replaces the
/// auto-derived set for that field after resolving names against `columns`
/// (falling back to PK/indexed refs). Primary-key columns are always forced
/// into the Bloom set.
///
/// # Errors
///
/// Returns [`RegistryError::UnknownColdMetadataColumn`] when an operator name
/// does not match any known column.
pub fn cold_metadata_config_for_registration(
    primary_key: &[ColumnRef],
    indexed_columns: &[ColumnRef],
    columns: &[SchemaColumn],
    pruning_columns: Option<&[String]>,
    bloom_filter_columns: Option<&[String]>,
) -> RegistryResult<ColdMetadataConfig> {
    let resolved_pruning = match pruning_columns {
        Some(names) => Some(resolve_operator_columns(
            "pruning_columns",
            names,
            columns,
            primary_key,
            indexed_columns,
        )?),
        None => None,
    };
    let resolved_bloom = match bloom_filter_columns {
        Some(names) => Some(resolve_operator_columns(
            "bloom_filter_columns",
            names,
            columns,
            primary_key,
            indexed_columns,
        )?),
        None => None,
    };
    Ok(cold_metadata_config_with_overrides(
        primary_key,
        indexed_columns,
        resolved_pruning.as_deref(),
        resolved_bloom.as_deref(),
    ))
}

fn cold_metadata_config_with_overrides(
    primary_key: &[ColumnRef],
    indexed_columns: &[ColumnRef],
    pruning_columns: Option<&[ColumnRef]>,
    bloom_filter_columns: Option<&[ColumnRef]>,
) -> ColdMetadataConfig {
    let stats_columns = match pruning_columns {
        Some(columns) => dedupe_column_refs(columns.iter()),
        None => dedupe_column_refs(indexed_columns.iter()),
    };
    let bloom_filter_columns = match bloom_filter_columns {
        Some(columns) => dedupe_column_refs(primary_key.iter().chain(columns.iter())),
        None => dedupe_column_refs(primary_key.iter().chain(indexed_columns.iter())),
    };
    let mut indexed_metadata = Vec::new();
    for (index, column) in primary_key
        .iter()
        .filter(|column| !column.name.trim().is_empty())
        .enumerate()
    {
        indexed_metadata.push(IndexedColumnMetadata::primary_key(
            column,
            (index + 1) as u32,
        ));
    }
    for (index, column) in stats_columns.iter().enumerate() {
        if !primary_key
            .iter()
            .any(|pk| pk.column_id == column.column_id)
        {
            indexed_metadata.push(IndexedColumnMetadata::secondary_index(
                column,
                (index + 1) as u32,
            ));
        }
    }

    ColdMetadataConfig {
        stats_columns,
        bloom_filter_columns,
        indexed_columns: indexed_metadata,
        ordered_indexes: Vec::new(),
    }
}

fn resolve_operator_columns(
    field: &'static str,
    names: &[String],
    columns: &[SchemaColumn],
    primary_key: &[ColumnRef],
    indexed_columns: &[ColumnRef],
) -> RegistryResult<Vec<ColumnRef>> {
    let mut resolved = Vec::with_capacity(names.len());
    for raw in names {
        let name = raw.trim();
        if name.is_empty() {
            return Err(RegistryError::UnknownColdMetadataColumn {
                field,
                column: raw.clone(),
            });
        }
        if let Some(column) = columns.iter().find(|column| column.name == name) {
            resolved.push(ColumnRef::new(column.column_id, column.name.clone()));
            continue;
        }
        if let Some(column) = primary_key
            .iter()
            .chain(indexed_columns.iter())
            .find(|column| column.name == name)
        {
            resolved.push(column.clone());
            continue;
        }
        return Err(RegistryError::UnknownColdMetadataColumn {
            field,
            column: name.to_string(),
        });
    }
    Ok(resolved)
}

fn dedupe_column_refs<'a>(columns: impl IntoIterator<Item = &'a ColumnRef>) -> Vec<ColumnRef> {
    let mut seen = BTreeSet::new();
    columns
        .into_iter()
        .filter(|column| !column.name.trim().is_empty())
        .filter(|column| seen.insert(column.column_id))
        .cloned()
        .collect()
}

fn options_object_mut(options: &mut Value) -> RegistryResult<&mut Map<String, Value>> {
    if options.is_null() {
        *options = Value::Object(Map::new());
    }
    options
        .as_object_mut()
        .ok_or_else(|| RegistryError::Spi("registry options must be a JSON object".to_string()))
}
