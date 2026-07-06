//! Change-log mirror orchestration for clean-schema managed tables.

use koldstore_common::{PrimaryKeyColumnShape, PrimaryKeyShape, SqlStatement};
use koldstore_mirror::{
    mirror_relation_for_source as storage_mirror_relation_for_source, plan_mirror_schema,
    statement::mirror_to_sql, MirrorStatement,
};

use crate::capture::{plan_mirror_capture, MirrorCapturePlan};
use crate::QualifiedTableName;

pub type MirrorResult<T> = Result<T, MirrorError>;

/// Change-log mirror planning error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MirrorError {
    /// A source table without a primary key cannot have a latest-state mirror.
    #[error("managed tables require a primary key before mirror artifacts are created")]
    MissingPrimaryKey,
    /// Mirror relation names must remain safe generated identifiers.
    #[error("invalid mirror relation `{0}`")]
    InvalidMirrorName(String),
    /// Primary-key columns in the source catalog should always be non-null.
    #[error("primary-key column `{0}` must be not null")]
    NullablePrimaryKey(String),
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
    /// DML capture trigger planning failed.
    #[error("{0}")]
    Capture(String),
}

impl From<koldstore_mirror::MirrorError> for MirrorError {
    fn from(error: koldstore_mirror::MirrorError) -> Self {
        match error {
            koldstore_mirror::MirrorError::MissingPrimaryKey => Self::MissingPrimaryKey,
            koldstore_mirror::MirrorError::InvalidMirrorName(value) => {
                Self::InvalidMirrorName(value)
            }
            koldstore_mirror::MirrorError::NullablePrimaryKey(column) => {
                Self::NullablePrimaryKey(column)
            }
            koldstore_mirror::MirrorError::InvalidColumn(column) => {
                Self::Sql(format!("invalid mirror storage column `{column}`"))
            }
        }
    }
}

/// Planned change-log mirror artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeLogMirrorPlan {
    /// Source application table.
    pub source_table: QualifiedTableName,
    /// Generated mirror table in the koldstore schema.
    pub mirror_table: QualifiedTableName,
    /// Collision probe executed before creating the mirror.
    pub collision_probe: SqlStatement,
    /// Exact-PK mirror table DDL.
    pub create_table: SqlStatement,
    /// Sequence cursor index for flush and change-feed scans.
    pub seq_index: SqlStatement,
    /// Row-age policy index.
    pub changed_at_index: SqlStatement,
    /// Transactional DML capture function/triggers.
    pub capture: MirrorCapturePlan,
    /// Idempotent mirror drop used by rollback/demigration.
    pub drop_table: SqlStatement,
}

impl ChangeLogMirrorPlan {
    /// Statements required to create the mirror after collision checks pass.
    #[must_use]
    pub fn create_statements(&self) -> Vec<&SqlStatement> {
        let mut statements = vec![&self.create_table, &self.seq_index, &self.changed_at_index];
        statements.extend(self.capture.create_statements());
        statements
    }
}

/// Plans a per-table change-log mirror from an exact primary-key shape.
///
/// # Errors
///
/// Returns an error when the key shape is empty, nullable, or the SQL statements
/// cannot be represented.
pub fn plan_change_log_mirror(
    source_table: &QualifiedTableName,
    primary_key: &PrimaryKeyShape,
) -> MirrorResult<ChangeLogMirrorPlan> {
    plan_change_log_mirror_from_columns(source_table, primary_key.columns())
}

/// Plans a per-table change-log mirror from ordered primary-key columns.
///
/// # Errors
///
/// Returns an error when the key columns are empty, nullable, or statement
/// metadata cannot be prepared.
pub fn plan_change_log_mirror_from_columns(
    source_table: &QualifiedTableName,
    columns: &[PrimaryKeyColumnShape],
) -> MirrorResult<ChangeLogMirrorPlan> {
    let source_name = source_table
        .as_table_name()
        .map_err(|error| MirrorError::InvalidMirrorName(error.to_string()))?;
    let mirror_storage = storage_mirror_relation_for_source(&source_name)?;
    let mirror_table = QualifiedTableName::from_table_name(mirror_storage.table_name());
    let schema_plan = plan_mirror_schema(&mirror_storage, columns)?;
    let capture = plan_mirror_capture(source_table, &mirror_table, columns)
        .map_err(|error| MirrorError::Capture(error.to_string()))?;

    Ok(ChangeLogMirrorPlan {
        source_table: source_table.clone(),
        mirror_table,
        collision_probe: mirror_sql(schema_plan.collision_probe)?,
        create_table: mirror_sql(schema_plan.create_table)?,
        seq_index: mirror_sql(schema_plan.seq_index)?,
        changed_at_index: mirror_sql(schema_plan.changed_at_index)?,
        drop_table: mirror_sql(schema_plan.drop_table)?,
        capture,
    })
}

/// Computes the default mirror relation for a source table.
///
/// # Errors
///
/// Returns an error when the generated relation would not be a safe PostgreSQL
/// identifier for pg-koldstore-owned DDL.
pub fn mirror_relation_for_source(
    source_table: &QualifiedTableName,
) -> MirrorResult<QualifiedTableName> {
    let source_name = source_table
        .as_table_name()
        .map_err(|error| MirrorError::InvalidMirrorName(error.to_string()))?;
    Ok(QualifiedTableName::from_table_name(
        storage_mirror_relation_for_source(&source_name)?.table_name(),
    ))
}

fn mirror_sql(statement: MirrorStatement) -> MirrorResult<SqlStatement> {
    mirror_to_sql(statement).map_err(|error| MirrorError::Sql(error.to_string()))
}
