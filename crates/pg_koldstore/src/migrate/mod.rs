//! Migration and demigration orchestration.

pub mod backfill;
pub mod columns;
pub mod constraints;
pub mod lock;
pub mod register;
pub mod rehydrate;
pub mod rollback;
pub mod scope;

use thiserror::Error;
use uuid::Uuid;

use crate::spi::SpiStatement;
use crate::sql::ddl::MigrateTableRequest;

pub use scope::SYSTEM_SCOPE_COLUMN;

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
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
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
        let value = value.trim();
        if value.is_empty() {
            return Err(MigrationError::InvalidTableName(value.to_string()));
        }

        let parts = value.split('.').collect::<Vec<_>>();
        match parts.as_slice() {
            [name] if is_safe_identifier(name) => Ok(Self {
                schema: None,
                name: (*name).to_string(),
            }),
            [schema, name] if is_safe_identifier(schema) && is_safe_identifier(name) => Ok(Self {
                schema: Some((*schema).to_string()),
                name: (*name).to_string(),
            }),
            _ => Err(MigrationError::InvalidTableName(value.to_string())),
        }
    }

    /// Returns a safely quoted SQL relation reference.
    #[must_use]
    pub fn quoted(&self) -> String {
        match &self.schema {
            Some(schema) => format!("\"{}\".\"{}\"", schema, self.name),
            None => format!("\"{}\"", self.name),
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
            let column = request
                .scope_column
                .as_deref()
                .unwrap_or(SYSTEM_SCOPE_COLUMN)
                .trim();
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

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
