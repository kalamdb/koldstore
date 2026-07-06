//! Migration entrypoint planning.

use koldstore_common::{is_safe_identifier, SqlStatement, TableKind};
use uuid::Uuid;

use crate::backfill::DEFAULT_BACKFILL_BATCH_ROWS;
use crate::jobs::{
    enqueue_migration_backfill_job_plan, MigrationBackfillJobRequest, MigrationBatchSize,
    MigrationJobEnqueuePlan, MigrationJobPhase,
};
use crate::order::{
    choose_migration_ordering, CatalogColumn, CatalogPrimaryKey, MigrationOrdering,
    MigrationOrderingRequest,
};
use crate::request::{MigrateTableRequest, MigrationError, MigrationResult};
use crate::QualifiedTableName;

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
    pub empty_table_probe: SqlStatement,
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
    let table = QualifiedTableName::parse(&request.table_name)
        .map_err(|error| MigrationError::InvalidTableName(error.to_string()))?;
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

    let empty_table_probe = SqlStatement::read(
        "check empty table",
        &format!("SELECT 1 FROM ONLY {} LIMIT 1", table.quoted()),
    )
    .map_err(|error| MigrationError::Sql(error.to_string()))?;

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
