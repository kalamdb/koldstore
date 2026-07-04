//! Demigration rehydrate helpers.

use thiserror::Error;

use crate::spi::SpiStatement;

use super::lock::{plan_migration_operation_lock, LockError, MigrationOperationLockPlan};
use super::QualifiedTableName;

/// Default demigration rehydrates cold rows.
pub const DEFAULT_REHYDRATE: bool = true;

/// Demigration execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemigrationMode {
    /// Rebuild the heap from the logical merged table before disabling hooks.
    Rehydrate,
    /// Disable management and leave cold artifacts as an archive.
    ArchiveDetach,
}

/// Options accepted by `koldstore.demigrate_table`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DemigrateOptions {
    /// Whether to rehydrate current logical rows into the heap.
    pub rehydrate: bool,
    /// Whether to mark cold artifacts deleted after a successful rehydrate.
    pub drop_cold: bool,
    /// Whether to remove pg-koldstore system columns from the heap.
    pub drop_system_columns: bool,
}

/// Catalog and storage context resolved before demigration planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemigrationContext {
    /// Target managed heap table.
    pub table: QualifiedTableName,
    /// PostgreSQL table oid.
    pub table_oid: u32,
    /// Cold object prefix for this table/scope.
    pub cold_object_prefix: String,
    /// Logical reader used for current hot+cold state.
    pub logical_reader_name: String,
}

/// Cold artifact handling after demigration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColdArtifactAction {
    /// Keep object-store artifacts as the default backup/archive behavior.
    Retain,
    /// Delete the cold object prefix only after rehydrate succeeds.
    DeleteAfterRehydrate {
        /// Object prefix to delete.
        prefix: String,
    },
}

/// Demigration planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DemigrationError {
    /// Cold deletion is unsafe without rehydration.
    #[error("drop_cold requires rehydrate => true")]
    DropColdWithoutRehydrate,
    /// Migration lock planning failed.
    #[error(transparent)]
    Lock(#[from] LockError),
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Complete pure demigration plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemigrationPlan {
    /// Chosen demigration mode.
    pub mode: DemigrationMode,
    /// Exclusive table management lock.
    pub lock: MigrationOperationLockPlan,
    /// Planned catalog/heap mutations after the lock is held.
    pub statements: Vec<SpiStatement>,
    /// Cold artifact action gated by rehydrate success.
    pub cold_artifact_action: ColdArtifactAction,
    /// Optional operator-facing warning.
    pub warning: Option<String>,
}

impl Default for DemigrateOptions {
    fn default() -> Self {
        Self {
            rehydrate: DEFAULT_REHYDRATE,
            drop_cold: false,
            drop_system_columns: false,
        }
    }
}

impl DemigrateOptions {
    /// Returns the planned demigration mode.
    #[must_use]
    pub fn mode(self) -> DemigrationMode {
        if self.rehydrate {
            DemigrationMode::Rehydrate
        } else {
            DemigrationMode::ArchiveDetach
        }
    }

    /// `drop_cold` is only safe after the rehydrate phase completed.
    #[must_use]
    pub fn requires_successful_rehydrate(self) -> bool {
        self.drop_cold
    }
}

/// Plans a full demigration flow.
///
/// # Errors
///
/// Returns an error when `drop_cold` is requested without `rehydrate`, or when
/// lock/statement metadata cannot be represented safely.
pub fn plan_demigration(
    context: DemigrationContext,
    options: DemigrateOptions,
) -> Result<DemigrationPlan, DemigrationError> {
    if options.drop_cold && !options.rehydrate {
        return Err(DemigrationError::DropColdWithoutRehydrate);
    }

    let mode = options.mode();
    let lock = plan_migration_operation_lock(&context.table, context.table_oid)?;
    let mut statements = Vec::new();

    if mode == DemigrationMode::Rehydrate {
        statements.extend(plan_rehydrate_heap(&context)?);
    }

    statements.push(plan_catalog_deactivation(context.table_oid)?);
    statements.push(plan_flush_deactivation(context.table_oid)?);

    if options.drop_system_columns {
        statements.push(plan_drop_system_columns(&context.table)?);
    }

    let cold_artifact_action = if options.drop_cold {
        ColdArtifactAction::DeleteAfterRehydrate {
            prefix: context.cold_object_prefix,
        }
    } else {
        ColdArtifactAction::Retain
    };

    let warning = (mode == DemigrationMode::ArchiveDetach).then(|| {
        "archive-detach demigration: cold-only rows will not be visible through normal table scans"
            .to_string()
    });

    Ok(DemigrationPlan {
        mode,
        lock,
        statements,
        cold_artifact_action,
        warning,
    })
}

/// Plans the rehydrate heap rebuild through the logical merge reader.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_rehydrate_heap(
    context: &DemigrationContext,
) -> Result<Vec<SpiStatement>, DemigrationError> {
    let table = context.table.quoted();
    let temp_table = format!("pg_koldstore_demigrate_{}", context.table_oid);
    let create_temp = SpiStatement::write(
        "demigrate rehydrate current rows",
        &format!(
            "CREATE TEMP TABLE {temp_table} AS SELECT * FROM {table} /* {} */ WHERE COALESCE(_deleted, false) = false",
            context.logical_reader_name
        ),
    )
    .map_err(|error| DemigrationError::Spi(error.to_string()))?;
    let truncate_hot = SpiStatement::write(
        "demigrate clear managed heap",
        &format!("TRUNCATE TABLE ONLY {table}"),
    )
    .map_err(|error| DemigrationError::Spi(error.to_string()))?;
    let restore_current = SpiStatement::write(
        "demigrate restore current rows",
        &format!("INSERT INTO {table} SELECT * FROM {temp_table}"),
    )
    .map_err(|error| DemigrationError::Spi(error.to_string()))?;

    Ok(vec![create_temp, truncate_hot, restore_current])
}

/// Plans metadata deactivation so planner/DML hooks treat the table as unmanaged.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_catalog_deactivation(table_oid: u32) -> Result<SpiStatement, DemigrationError> {
    let _ = table_oid;
    SpiStatement::write(
        "demigrate deactivate catalog metadata",
        "UPDATE koldstore.schemas SET active = false WHERE table_oid = $1 AND active = true",
    )
    .map_err(|error| DemigrationError::Spi(error.to_string()))
}

/// Plans cancellation of pending/running flush jobs for a demigrated table.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_flush_deactivation(table_oid: u32) -> Result<SpiStatement, DemigrationError> {
    let _ = table_oid;
    SpiStatement::write(
        "demigrate cancel flush jobs",
        "UPDATE koldstore.jobs SET status = 'cancelled', updated_at = now() WHERE table_oid = $1 AND status IN ('pending', 'running')",
    )
    .map_err(|error| DemigrationError::Spi(error.to_string()))
}

fn plan_drop_system_columns(table: &QualifiedTableName) -> Result<SpiStatement, DemigrationError> {
    SpiStatement::write(
        "demigrate drop system columns",
        &format!(
            "ALTER TABLE ONLY {} DROP COLUMN IF EXISTS \"_seq\", DROP COLUMN IF EXISTS \"_commit_seq\", DROP COLUMN IF EXISTS \"_deleted\"",
            table.quoted()
        ),
    )
    .map_err(|error| DemigrationError::Spi(error.to_string()))
}
