//! Extension bootstrap DDL loaded from the canonical install script.

use std::path::Path;

/// Full bootstrap install plan for the `koldstore` schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapPlan {
    /// SQL statements in dependency order.
    pub statements: Vec<String>,
}

impl BootstrapPlan {
    /// Loads bootstrap statements from a SQL migration file.
    ///
    /// Statements are split on semicolons while preserving order.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read.
    pub fn from_sql_file(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let sql = std::fs::read_to_string(path)?;
        Ok(Self::from_sql(&sql))
    }

    /// Parses bootstrap statements from SQL text.
    #[must_use]
    pub fn from_sql(sql: &str) -> Self {
        let statements = sql
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
            .map(ToString::to_string)
            .collect();
        Self { statements }
    }
}
