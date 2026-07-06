//! Migration and demigration orchestration.

pub mod backfill;
pub mod constraints;
pub mod jobs;
pub mod lock;
pub mod mirror;
pub mod order;
pub mod register;
pub mod rehydrate;
pub mod rollback;
pub mod scope;

use koldstore_core::{is_safe_identifier, TableKind, TableName};
use thiserror::Error;
use uuid::Uuid;

use crate::spi::SpiStatement;
use crate::sql::ddl::MigrateTableRequest;

use self::backfill::DEFAULT_BACKFILL_BATCH_ROWS;
use self::jobs::{
    enqueue_migration_backfill_job_plan, MigrationBackfillJobRequest, MigrationBatchSize,
    MigrationJobEnqueuePlan, MigrationJobPhase,
};
use self::order::{
    choose_migration_ordering, CatalogColumn, CatalogPrimaryKey, MigrationOrdering,
    MigrationOrderingRequest,
};

/// Migration planning result.
pub type MigrationResult<T> = Result<T, MigrationError>;

/// Migration request validation or planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MigrationError {
    /// Table name is blank or not a simple qualified identifier.
    #[error("invalid table_name `{0}`")]
    InvalidTableName(String),
    /// Table type must be `shared` or `user`.
    #[error("unsupported table_type `{0}`")]
    UnsupportedTableType(String),
    /// Storage name is blank.
    #[error("storage_name cannot be blank")]
    BlankStorageName,
    /// Scope column is blank or not a simple identifier.
    #[error("invalid scope_column `{0}`")]
    InvalidScopeColumn(String),
    /// User-scoped clean-schema tables must use an application-owned scope column.
    #[error("user-scoped clean-schema migration requires scope_column")]
    MissingScopeColumn,
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
    /// Existing-table ordering metadata is insufficient.
    #[error("{0}")]
    Ordering(String),
    /// Migration job planning failed.
    #[error("{0}")]
    Job(String),
}

/// Parsed table name accepted by `koldstore.migrate_table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedTableName {
    /// Optional schema name.
    pub schema: Option<String>,
    /// Relation name.
    pub name: String,
}

impl QualifiedTableName {
    /// Parses an unquoted one- or two-part PostgreSQL identifier.
    ///
    /// # Errors
    ///
    /// Returns an error for blank, multipart, or unsafe identifier text.
    pub fn parse(value: &str) -> MigrationResult<Self> {
        let table = TableName::parse(value).map_err(|error| match error {
            koldstore_core::KoldstoreError::InvalidIdentifier { value, .. } => {
                MigrationError::InvalidTableName(value)
            }
            other => MigrationError::InvalidTableName(other.to_string()),
        })?;
        Ok(Self {
            schema: table.schema().map(str::to_string),
            name: table.relation().to_string(),
        })
    }

    /// Returns the normalized [`TableName`] for this relation.
    ///
    /// # Errors
    ///
    /// Returns an error when schema/name components are no longer valid.
    pub fn as_table_name(&self) -> MigrationResult<TableName> {
        TableName::parse(self.display_name()).map_err(|error| match error {
            koldstore_core::KoldstoreError::InvalidIdentifier { value, .. } => {
                MigrationError::InvalidTableName(value)
            }
            other => MigrationError::InvalidTableName(other.to_string()),
        })
    }

    /// Builds a [`QualifiedTableName`] from a validated [`TableName`].
    #[must_use]
    pub fn from_table_name(table: &TableName) -> Self {
        Self {
            schema: table.schema().map(str::to_string),
            name: table.relation().to_string(),
        }
    }

    /// Returns the mirror relation for this qualified table name.
    ///
    /// # Errors
    ///
    /// Returns an error when schema/name components are no longer valid.
    pub fn as_mirror_relation(&self) -> MigrationResult<koldstore_mirror::MirrorRelation> {
        Ok(koldstore_mirror::MirrorRelation::new(self.as_table_name()?))
    }

    /// Returns a safely quoted SQL relation reference.
    #[must_use]
    pub fn quoted(&self) -> String {
        self.as_table_name()
            .map(|table| table.quoted())
            .unwrap_or_else(|_| match &self.schema {
                Some(schema) => format!("\"{schema}\".\"{}\"", self.name),
                None => format!("\"{}\"", self.name),
            })
    }

    fn display_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}", self.name),
            None => self.name.clone(),
        }
    }
}

/// Catalog context resolved before migration planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationTableContext {
    /// OID of the target table.
    pub table_oid: u32,
    /// Registered storage id.
    pub storage_id: Uuid,
}

/// Planned work for the empty-table migration entrypoint.
#[derive(Debug, Clone, PartialEq)]
pub struct EmptyTableMigrationPlan {
    /// Target table.
    pub table: QualifiedTableName,
    /// OID of the target table.
    pub table_oid: u32,
    /// Registered storage id.
    pub storage_id: Uuid,
    /// Effective user scope column, if user-scoped.
    pub effective_scope_column: Option<String>,
    /// Read-only probe; any returned row means the greenfield-only path must stop.
    pub empty_table_probe: SpiStatement,
}

/// Catalog metadata for a populated table migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingTableCatalog {
    /// Primary-key metadata.
    pub primary_key: CatalogPrimaryKey,
    /// Column metadata.
    pub columns: Vec<CatalogColumn>,
    /// Simple indexed or constrained columns eligible for cold metadata.
    pub indexed_columns: Vec<String>,
}

/// Planned work for an async existing-table migration.
#[derive(Debug, Clone, PartialEq)]
pub struct ExistingTableMigrationPlan {
    /// Target table.
    pub table: QualifiedTableName,
    /// OID of the target table.
    pub table_oid: u32,
    /// Registered storage id.
    pub storage_id: Uuid,
    /// Effective user scope column, if user-scoped.
    pub effective_scope_column: Option<String>,
    /// Oldest-to-newest ordering selected for backfill.
    pub ordering: MigrationOrdering,
    /// Bounded backfill batch size.
    pub backfill_batch_size: MigrationBatchSize,
    /// Initial durable job phase.
    pub initial_phase: MigrationJobPhase,
    /// Async migration backfill job enqueue plan.
    pub backfill_job: MigrationJobEnqueuePlan,
}

/// Plans the first migration step for an empty greenfield table.
///
/// # Errors
///
/// Returns an error when request arguments are unsupported or unsafe to turn
/// into catalog statements.
pub fn plan_empty_table_migration(
    request: &MigrateTableRequest,
    context: MigrationTableContext,
) -> MigrationResult<EmptyTableMigrationPlan> {
    let table = QualifiedTableName::parse(&request.table_name)?;
    if !request.has_supported_table_type() {
        return Err(MigrationError::UnsupportedTableType(
            request.table_type.clone(),
        ));
    }
    if request.storage_name.trim().is_empty() {
        return Err(MigrationError::BlankStorageName);
    }

    let effective_scope_column = match request.table_type.as_str() {
        "shared" => None,
        "user" => {
            let Some(column) = request.scope_column.as_deref().map(str::trim) else {
                return Err(MigrationError::MissingScopeColumn);
            };
            if column.is_empty() {
                return Err(MigrationError::MissingScopeColumn);
            }
            if !is_safe_identifier(column) {
                return Err(MigrationError::InvalidScopeColumn(column.to_string()));
            }
            Some(column.to_string())
        }
        _ => unreachable!("table type was checked above"),
    };

    let empty_table_probe = SpiStatement::read(
        "check empty table",
        &format!("SELECT 1 FROM ONLY {} LIMIT 1", table.quoted()),
    )
    .map_err(|error| MigrationError::Spi(error.to_string()))?;

    Ok(EmptyTableMigrationPlan {
        table,
        table_oid: context.table_oid,
        storage_id: context.storage_id,
        effective_scope_column,
        empty_table_probe,
    })
}

/// Plans migration of a populated table into async backfill.
///
/// # Errors
///
/// Returns an error when request arguments are invalid, the table has no stable
/// oldest-to-newest order, or job statement metadata cannot be prepared.
pub fn plan_existing_table_migration(
    request: &MigrateTableRequest,
    context: MigrationTableContext,
    catalog: ExistingTableCatalog,
    job_id: Uuid,
) -> MigrationResult<ExistingTableMigrationPlan> {
    let base = plan_empty_table_migration(request, context)?;
    let explicit_order_column = request
        .options
        .get("order_column")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(ToString::to_string);
    let ordering = choose_migration_ordering(&MigrationOrderingRequest {
        primary_key: catalog.primary_key,
        columns: catalog.columns,
        explicit_order_column,
    })
    .map_err(|error| MigrationError::Ordering(error.to_string()))?;
    let backfill_batch_size = migration_backfill_batch_size(request)?;
    let table_type = request
        .table_type
        .parse::<TableKind>()
        .map_err(|error| MigrationError::UnsupportedTableType(error.to_string()))?;
    let backfill_job = enqueue_migration_backfill_job_plan(MigrationBackfillJobRequest::new(
        job_id,
        base.table_oid,
        &base.table,
        table_type,
        base.storage_id,
        base.effective_scope_column.clone(),
        &ordering,
        backfill_batch_size,
        request.flush_policy.clone(),
    ))
    .map_err(|error| MigrationError::Job(error.to_string()))?;

    Ok(ExistingTableMigrationPlan {
        table: base.table,
        table_oid: base.table_oid,
        storage_id: base.storage_id,
        effective_scope_column: base.effective_scope_column,
        ordering,
        backfill_batch_size,
        initial_phase: MigrationJobPhase::InitializeMirror,
        backfill_job,
    })
}

fn migration_backfill_batch_size(
    request: &MigrateTableRequest,
) -> MigrationResult<MigrationBatchSize> {
    let configured = request
        .options
        .get("backfill_batch_size")
        .and_then(serde_json::Value::as_u64)
        .map_or(DEFAULT_BACKFILL_BATCH_ROWS, |value| value as usize);

    MigrationBatchSize::new(configured).map_err(|error| MigrationError::Job(error.to_string()))
}
