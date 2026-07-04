//! System column definitions.

use crate::spi::SpiStatement;
use crate::sql::session;

use super::{MigrationError, MigrationResult, QualifiedTableName};

/// Required system columns.
pub const REQUIRED_SYSTEM_COLUMNS: &[&str] = &["_seq", "_commit_seq", "_deleted"];

/// Planned system-column DDL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemColumnPlan {
    /// Columns that will exist after applying this plan.
    pub columns: Vec<&'static str>,
    /// DDL statement that adds missing pg-koldstore system columns.
    pub statement: SpiStatement,
}

/// Existing-table system-column preparation plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingTableSystemColumnPreparePlan {
    /// Columns that will exist after applying this plan.
    pub columns: Vec<&'static str>,
    /// DDL statements to run before async backfill starts.
    pub statements: Vec<SpiStatement>,
}

/// Existing-table system-column finalization plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingTableSystemColumnFinalizePlan {
    /// DDL statement to run after every old row has system values.
    pub statement: SpiStatement,
}

/// Returns all system columns for a migration.
#[must_use]
pub fn system_columns(user_scoped_without_app_column: bool) -> Vec<&'static str> {
    let mut columns = REQUIRED_SYSTEM_COLUMNS.to_vec();
    if user_scoped_without_app_column {
        columns.push("_user_id");
    }
    columns
}

/// Builds the DDL plan for adding pg-koldstore system columns.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_system_column_adds(
    table: &QualifiedTableName,
    user_scoped_without_app_column: bool,
) -> MigrationResult<SystemColumnPlan> {
    let columns = system_columns(user_scoped_without_app_column);
    let mut fragments = vec![
        format!(
            "ADD COLUMN IF NOT EXISTS \"_seq\" bigint NOT NULL {}",
            session::system_seq_default_clause()
        ),
        "ADD COLUMN IF NOT EXISTS \"_commit_seq\" bigint NOT NULL DEFAULT 0".to_string(),
        "ADD COLUMN IF NOT EXISTS \"_deleted\" boolean NOT NULL DEFAULT false".to_string(),
    ];

    if user_scoped_without_app_column {
        fragments.push("ADD COLUMN IF NOT EXISTS \"_user_id\" text".to_string());
    }

    let statement = SpiStatement::write(
        "add system columns",
        &format!(
            "ALTER TABLE ONLY {}\n    {}",
            table.quoted(),
            fragments.join(",\n    ")
        ),
    )
    .map_err(|error| MigrationError::Spi(error.to_string()))?;

    Ok(SystemColumnPlan { columns, statement })
}

/// Builds DDL for populated tables without forcing an immediate heap rewrite.
///
/// The columns are added nullable first, then defaults are installed for future
/// writes. Existing rows stay NULL until the async backfill job assigns ordered
/// values in bounded batches.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_existing_table_system_column_prepare(
    table: &QualifiedTableName,
    user_scoped_without_app_column: bool,
) -> MigrationResult<ExistingTableSystemColumnPreparePlan> {
    let columns = system_columns(user_scoped_without_app_column);
    let mut add_fragments = vec![
        "ADD COLUMN IF NOT EXISTS \"_seq\" bigint".to_string(),
        "ADD COLUMN IF NOT EXISTS \"_commit_seq\" bigint".to_string(),
        "ADD COLUMN IF NOT EXISTS \"_deleted\" boolean".to_string(),
    ];
    if user_scoped_without_app_column {
        add_fragments.push("ADD COLUMN IF NOT EXISTS \"_user_id\" text".to_string());
    }

    let statements = [
        SpiStatement::write(
            "add nullable system columns for existing table",
            &format!(
                "ALTER TABLE ONLY {}\n    {}",
                table.quoted(),
                add_fragments.join(",\n    ")
            ),
        ),
        SpiStatement::write(
            "set system column defaults for future writes",
            &format!(
                "ALTER TABLE ONLY {}\n    ALTER COLUMN \"_seq\" SET {},\n    ALTER COLUMN \"_commit_seq\" SET DEFAULT nextval('koldstore.global_commit_seq'::regclass),\n    ALTER COLUMN \"_deleted\" SET DEFAULT false",
                table.quoted(),
                session::system_seq_default_clause(),
            ),
        ),
    ]
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .map_err(|error| MigrationError::Spi(error.to_string()))?;

    Ok(ExistingTableSystemColumnPreparePlan {
        columns,
        statements,
    })
}

/// Builds DDL that enforces system-column invariants after async backfill.
///
/// # Errors
///
/// Returns an error when SPI statement metadata cannot be prepared.
pub fn plan_existing_table_system_column_finalize(
    table: &QualifiedTableName,
) -> MigrationResult<ExistingTableSystemColumnFinalizePlan> {
    let statement = SpiStatement::write(
        "finalize existing table system columns",
        &format!(
            "ALTER TABLE ONLY {}\n    ALTER COLUMN \"_seq\" SET NOT NULL,\n    ALTER COLUMN \"_commit_seq\" SET NOT NULL,\n    ALTER COLUMN \"_deleted\" SET NOT NULL",
            table.quoted(),
        ),
    )
    .map_err(|error| MigrationError::Spi(error.to_string()))?;

    Ok(ExistingTableSystemColumnFinalizePlan { statement })
}
