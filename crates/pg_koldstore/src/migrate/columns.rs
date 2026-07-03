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
