//! Schema registry insertion helpers.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use koldstore_common::{dedupe_nonblank, ManageTableOptions};

use koldstore_common::SqlStatement;
use koldstore_common::{
    PgCollation, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
    PrimaryKeyShape,
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
    /// User-scoped table metadata is missing its scope column.
    #[error("user-scoped manage_table requires scope_column")]
    MissingScopeColumn,
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Cold metadata columns derived from preserved hot indexes and primary keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdMetadataCandidates {
    /// Columns worth recording min/max/null-count style statistics for.
    pub stats_columns: Vec<String>,
    /// Columns configured for Parquet bloom filters.
    pub bloom_filter_columns: Vec<String>,
    /// Columns worth considering for bloom filters.
    pub bloom_candidate_columns: Vec<String>,
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
    pub fn primary_key(column: impl Into<String>, ordinal: u32) -> Self {
        Self {
            column: column.into(),
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
    pub fn secondary_index(column: impl Into<String>, ordinal: u32) -> Self {
        Self {
            column: column.into(),
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
    pub columns: Vec<String>,
    /// Whether the ordered key is unique.
    pub unique: bool,
}

/// Typed cold metadata configuration stored in schema options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdMetadataConfig {
    /// Columns worth recording min/max/null-count style statistics for.
    pub stats_columns: Vec<String>,
    /// Columns configured for Parquet bloom filters.
    pub bloom_filter_columns: Vec<String>,
    /// Backward-compatible alias for readers still looking for candidate columns.
    pub bloom_candidate_columns: Vec<String>,
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
    pub table_oid: u32,
    /// Table type.
    pub table_type: String,
    /// Storage id.
    pub storage_id: uuid::Uuid,
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
    pub primary_key: Vec<String>,
    /// Application column metadata.
    pub columns: Vec<SchemaColumn>,
    /// Indexed columns used as cold stats/bloom candidates.
    pub indexed_columns: Vec<String>,
    /// Captured type support/coercion metadata.
    pub type_matrix: Value,
    /// Additional manage-table options.
    pub options: ManageTableOptions,
}

/// Prepared JSON metadata for `koldstore.schemas`.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedRegistrationMetadata {
    /// Table oid.
    pub table_oid: u32,
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
    pub storage_id: Uuid,
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
        self.table_oid != 0
            && matches!(self.table_type.as_str(), "shared" | "user")
            && self.storage_id != Uuid::nil()
            && !self.primary_key.is_empty()
            && self
                .primary_key
                .iter()
                .all(|column| !column.trim().is_empty())
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
        if self.table_oid == 0 {
            return Err(RegistryError::MissingTableOid);
        }
        if !matches!(self.table_type.as_str(), "shared" | "user") {
            return Err(RegistryError::UnsupportedTableType(self.table_type.clone()));
        }
        if self.storage_id == Uuid::nil() {
            return Err(RegistryError::MissingStorageId);
        }
        if self.primary_key.is_empty()
            || self
                .primary_key
                .iter()
                .any(|column| column.trim().is_empty())
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
        let cold_metadata = cold_metadata_config(&self.primary_key, &self.indexed_columns);
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
            storage_id: self.storage_id,
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
                column.name.clone(),
                column.pg_type,
                column.catalog_type_name(),
                true,
                false,
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
pub fn plan_activate_managed_schema(table_oid: u32) -> RegistryResult<SqlStatement> {
    if table_oid == 0 {
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
pub fn primary_key_shape_probe_plan(table_oid: u32) -> RegistryResult<SqlStatement> {
    if table_oid == 0 {
        return Err(RegistryError::MissingTableOid);
    }

    SqlStatement::read(
        "capture primary-key shape",
        r#"
SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object(
            'column', a.attname,
            'ordinal', key_position.ordinality,
            'type_oid', a.atttypid::bigint,
            'type_name', format_type(COALESCE(NULLIF(t.typbasetype, 0), a.atttypid), a.atttypmod),
            'typmod', a.atttypmod,
            'collation', CASE
                WHEN coll.oid IS NULL OR coll.collname = 'default' THEN NULL
                ELSE format('%I.%I', coll_ns.nspname, coll.collname)
            END,
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
    let columns = rows
        .into_iter()
        .map(|row| {
            Ok(PrimaryKeyColumnShape::new(
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
    primary_key: &[String],
    indexed_columns: &[String],
) -> ColdMetadataCandidates {
    let config = cold_metadata_config(primary_key, indexed_columns);

    ColdMetadataCandidates {
        stats_columns: config.stats_columns,
        bloom_filter_columns: config.bloom_filter_columns,
        bloom_candidate_columns: config.bloom_candidate_columns,
    }
}

/// Builds typed cold metadata configuration from PK and indexed columns.
#[must_use]
pub fn cold_metadata_config(
    primary_key: &[String],
    indexed_columns: &[String],
) -> ColdMetadataConfig {
    let stats_columns = dedupe_nonblank(indexed_columns.iter().map(String::as_str));
    let bloom_filter_columns = dedupe_nonblank(
        primary_key
            .iter()
            .chain(indexed_columns)
            .map(String::as_str),
    );
    let mut indexed_metadata = Vec::new();
    for (index, column) in primary_key
        .iter()
        .map(String::as_str)
        .filter(|column| !column.trim().is_empty())
        .enumerate()
    {
        indexed_metadata.push(IndexedColumnMetadata::primary_key(
            column.trim(),
            (index + 1) as u32,
        ));
    }
    for (index, column) in stats_columns.iter().map(String::as_str).enumerate() {
        if !primary_key.iter().any(|pk| pk == column) {
            indexed_metadata.push(IndexedColumnMetadata::secondary_index(
                column,
                (index + 1) as u32,
            ));
        }
    }

    ColdMetadataConfig {
        stats_columns,
        bloom_candidate_columns: bloom_filter_columns.clone(),
        bloom_filter_columns,
        indexed_columns: indexed_metadata,
        ordered_indexes: Vec::new(),
    }
}

fn options_object_mut(options: &mut Value) -> RegistryResult<&mut Map<String, Value>> {
    if options.is_null() {
        *options = Value::Object(Map::new());
    }
    options
        .as_object_mut()
        .ok_or_else(|| RegistryError::Spi("registry options must be a JSON object".to_string()))
}
