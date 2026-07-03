//! Schema registry insertion helpers.

use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::spi::SpiStatement;
use koldstore_catalog::{SchemaColumn, TypeMatrix};

/// Initial schema version for a managed table.
pub const INITIAL_SCHEMA_VERSION: u32 = 1;
/// Type matrix JSON schema version stored in `system.schemas`.
pub const TYPE_MATRIX_CAPTURE_VERSION: u32 = 1;

const REGISTER_SCHEMA_SQL: &str = r#"
INSERT INTO system.schemas AS s (
    id,
    table_oid,
    version,
    active,
    table_type,
    columns,
    primary_key,
    scope_column,
    indexed_columns,
    type_matrix,
    options,
    storage_id
)
VALUES (
    $1,
    $2,
    $3,
    true,
    $4,
    $5::jsonb,
    $6::jsonb,
    $7,
    $8::jsonb,
    $9::jsonb,
    $10::jsonb,
    $11
)
ON CONFLICT (table_oid, version) DO UPDATE
SET active = EXCLUDED.active,
    table_type = EXCLUDED.table_type,
    columns = EXCLUDED.columns,
    primary_key = EXCLUDED.primary_key,
    scope_column = EXCLUDED.scope_column,
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
    /// User-scoped table metadata is missing its scope column.
    #[error("user-scoped table requires scope_column")]
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
    /// Columns worth considering for bloom filters.
    pub bloom_candidate_columns: Vec<String>,
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
    /// Primary key columns.
    pub primary_key: Vec<String>,
    /// Column metadata including app and pg-koldstore system columns.
    pub columns: Vec<SchemaColumn>,
    /// Indexed columns used as cold stats/bloom candidates.
    pub indexed_columns: Vec<String>,
    /// Captured type support/coercion metadata.
    pub type_matrix: Value,
    /// Optional flush policy.
    pub flush_policy: Option<String>,
    /// Additional migration options.
    pub options: Value,
}

/// Prepared JSON metadata for `system.schemas`.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedRegistrationMetadata {
    /// Table oid.
    pub table_oid: u32,
    /// Schema version.
    pub version: u32,
    /// Managed table type.
    pub table_type: String,
    /// Serialized app and system columns.
    pub columns: Value,
    /// Serialized preserved primary key columns.
    pub primary_key: Value,
    /// Effective scope column.
    pub scope_column: Option<String>,
    /// Serialized indexed column names.
    pub indexed_columns: Value,
    /// Type matrix JSON.
    pub type_matrix: Value,
    /// Options JSON including flush policy.
    pub options: Value,
    /// Storage registration id.
    pub storage_id: Uuid,
}

/// Planned `system.schemas` catalog insertion.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaRegistryPlan {
    /// Schema registry row id to bind as `$1`.
    pub schema_id: Uuid,
    /// Prepared metadata values to bind as `$2` through `$11`.
    pub metadata: PreparedRegistrationMetadata,
    /// Parameterized SPI statement.
    pub statement: SpiStatement,
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

    /// Serializes registry metadata into the shape written to `system.schemas`.
    ///
    /// # Errors
    ///
    /// Returns an error when validation fails.
    pub fn prepare(&self) -> RegistryResult<PreparedRegistrationMetadata> {
        self.validate()?;

        let mut options = self.options.clone();
        if let Some(flush_policy) = self.flush_policy.as_deref().map(str::trim) {
            if !flush_policy.is_empty() {
                let object = options_object_mut(&mut options)?;
                object.insert(
                    "flush_policy".to_string(),
                    Value::String(flush_policy.to_string()),
                );
            }
        }
        let cold_metadata = cold_metadata_candidates(&self.primary_key, &self.indexed_columns);
        if !cold_metadata.stats_columns.is_empty()
            || !cold_metadata.bloom_candidate_columns.is_empty()
        {
            let object = options_object_mut(&mut options)?;
            object.insert(
                "cold_metadata".to_string(),
                serde_json::json!({
                    "stats_columns": cold_metadata.stats_columns,
                    "bloom_candidate_columns": cold_metadata.bloom_candidate_columns,
                }),
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
            table_type: self.table_type.clone(),
            columns: serde_json::to_value(&self.columns).unwrap_or_else(|_| Value::Array(vec![])),
            primary_key: serde_json::json!(self.primary_key),
            scope_column: self
                .scope_column
                .as_deref()
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .map(ToString::to_string),
            indexed_columns: serde_json::json!(self.indexed_columns),
            type_matrix,
            options,
            storage_id: self.storage_id,
        })
    }
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
    let statement = SpiStatement::write("register managed table schema", REGISTER_SCHEMA_SQL)
        .map_err(|error| RegistryError::Spi(error.to_string()))?;

    Ok(SchemaRegistryPlan {
        schema_id,
        metadata,
        statement,
    })
}

/// Captures supported-type metadata for the columns being registered.
#[must_use]
pub fn capture_type_matrix(columns: &[SchemaColumn]) -> Value {
    let matrix = TypeMatrix::postgres_15_default();
    let columns = columns
        .iter()
        .map(|column| {
            let support = matrix.support_for(canonical_type_name(&column.type_name));
            match support.diagnostic {
                Some(diagnostic) => serde_json::json!({
                    "name": column.name,
                    "type_name": column.type_name,
                    "supported": support.supported,
                    "diagnostic": diagnostic,
                }),
                None => serde_json::json!({
                    "name": column.name,
                    "type_name": column.type_name,
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
    let stats_columns = dedupe_nonblank(indexed_columns.iter().map(String::as_str));
    let bloom_candidate_columns = dedupe_nonblank(
        primary_key
            .iter()
            .chain(indexed_columns)
            .map(String::as_str),
    );

    ColdMetadataCandidates {
        stats_columns,
        bloom_candidate_columns,
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

fn dedupe_nonblank<'a>(values: impl Iterator<Item = &'a str>) -> Vec<String> {
    values.fold(Vec::new(), |mut columns, value| {
        let column = value.trim();
        if !column.is_empty() && !columns.iter().any(|existing| existing == column) {
            columns.push(column.to_string());
        }
        columns
    })
}

fn canonical_type_name(type_name: &str) -> &str {
    match type_name {
        "boolean" => "bool",
        "smallint" => "int2",
        "integer" => "int4",
        "bigint" => "int8",
        "real" => "float4",
        "double precision" => "float8",
        "character varying" => "varchar",
        "timestamp with time zone" => "timestamptz",
        other => other,
    }
}
