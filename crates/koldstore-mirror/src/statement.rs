//! Pg-free SQL statement contract for mirror storage operations.

pub use koldstore_common::SqlParamType;

/// Mirror statement access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorAccess {
    /// Statement only reads mirror storage.
    ReadOnly,
    /// Statement mutates mirror storage.
    ReadWrite,
}

/// Planned mirror storage statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorStatement {
    /// Human-readable operation label.
    pub label: &'static str,
    /// SQL text with bind placeholders owned by the caller.
    pub sql: String,
    /// Expected access mode.
    pub access: MirrorAccess,
    /// Bind parameter types by one-based placeholder position.
    pub param_types: Vec<SqlParamType>,
}

impl MirrorStatement {
    /// Creates a read-only mirror statement.
    #[must_use]
    pub fn read(label: &'static str, sql: impl Into<String>) -> Self {
        Self::read_with_params(label, sql, [])
    }

    /// Creates a read-only mirror statement with parameter metadata.
    #[must_use]
    pub fn read_with_params(
        label: &'static str,
        sql: impl Into<String>,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> Self {
        Self {
            label,
            sql: sql.into(),
            access: MirrorAccess::ReadOnly,
            param_types: param_types.into(),
        }
    }

    /// Creates a read-write mirror statement.
    #[must_use]
    pub fn write(label: &'static str, sql: impl Into<String>) -> Self {
        Self::write_with_params(label, sql, [])
    }

    /// Creates a read-write mirror statement with parameter metadata.
    #[must_use]
    pub fn write_with_params(
        label: &'static str,
        sql: impl Into<String>,
        param_types: impl Into<Vec<SqlParamType>>,
    ) -> Self {
        Self {
            label,
            sql: sql.into(),
            access: MirrorAccess::ReadWrite,
            param_types: param_types.into(),
        }
    }
}

/// Converts a mirror statement into a pg-free SQL plan.
///
/// # Errors
///
/// Returns an error when statement metadata is blank or invalid.
pub fn mirror_to_sql(
    statement: MirrorStatement,
) -> koldstore_common::SqlResult<koldstore_common::SqlStatement> {
    let param_types = statement.param_types;
    match statement.access {
        MirrorAccess::ReadOnly => koldstore_common::SqlStatement::read_with_params(
            statement.label,
            &statement.sql,
            param_types,
        ),
        MirrorAccess::ReadWrite => koldstore_common::SqlStatement::write_with_params(
            statement.label,
            &statement.sql,
            param_types,
        ),
    }
}
