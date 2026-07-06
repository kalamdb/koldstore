//! Existing-row mirror initialization settings and planning.

use koldstore_core::PrimaryKeyColumnShape;
use thiserror::Error;

use crate::spi::SpiStatement;

use super::{jobs::MigrationBatchSize, order::MigrationOrdering, QualifiedTableName};

/// Backfill batch sizing default.
pub const DEFAULT_BACKFILL_BATCH_ROWS: usize = 10_000;

/// Mirror initialization planning result.
pub type MirrorInitializationResult<T> = Result<T, MirrorInitializationError>;

/// Mirror initialization planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MirrorInitializationError {
    /// Existing-row initialization needs at least one primary-key column.
    #[error("mirror initialization requires at least one primary-key column")]
    MissingPrimaryKey,
    /// Identifier is unsafe to quote.
    #[error("invalid mirror initialization identifier `{0}`")]
    InvalidIdentifier(String),
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

/// Planned bounded initialization batch from hot rows into the mirror.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorInitializationBatchPlan {
    /// Source user table.
    pub table: QualifiedTableName,
    /// Target change-log mirror table.
    pub mirror_table: QualifiedTableName,
    /// Oldest-to-newest scan ordering.
    pub ordering: MigrationOrdering,
    /// Maximum base rows scanned by this batch.
    pub batch_size: MigrationBatchSize,
    /// SQL statement returning scanned and initialized row counts.
    pub statement: SpiStatement,
}

/// Plans a bounded mirror initialization batch for populated-table enablement.
///
/// The statement reads a stable batch of base rows, inserts missing mirror rows
/// as `op = 1`, and uses `ON CONFLICT DO NOTHING` so concurrent/newer DML
/// captured by triggers is never overwritten by initialization.
///
/// # Errors
///
/// Returns an error when the primary key is missing, an identifier is unsafe, or
/// statement metadata cannot be represented by the SPI helper.
pub fn plan_mirror_initialization_batch(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
    ordering: MigrationOrdering,
    batch_size: MigrationBatchSize,
) -> MirrorInitializationResult<MirrorInitializationBatchPlan> {
    if primary_key.is_empty() {
        return Err(MirrorInitializationError::MissingPrimaryKey);
    }
    if !is_safe_identifier(&ordering.column) {
        return Err(MirrorInitializationError::InvalidIdentifier(
            ordering.column,
        ));
    }

    let pk_columns = primary_key
        .iter()
        .map(|column| {
            let column = column.column().as_str();
            if is_safe_identifier(column) {
                Ok(quote_ident(column))
            } else {
                Err(MirrorInitializationError::InvalidIdentifier(
                    column.to_string(),
                ))
            }
        })
        .collect::<MirrorInitializationResult<Vec<_>>>()?;
    let hot_pk_columns = pk_columns
        .iter()
        .map(|column| format!("hot.{column}"))
        .collect::<Vec<_>>();
    let mirror_pk_columns = pk_columns
        .iter()
        .map(|column| format!("mirror.{column}"))
        .collect::<Vec<_>>();
    let join_predicate = hot_pk_columns
        .iter()
        .zip(mirror_pk_columns.iter())
        .map(|(hot, mirror)| format!("{mirror} = {hot}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mirror_missing_predicate = format!("{} IS NULL", mirror_pk_columns[0]);
    let order_column = quote_ident(&ordering.column);
    let order_column_ref = format!("hot.{order_column}");
    let mut insert_columns = pk_columns.clone();
    insert_columns.extend([
        "\"seq\"".to_string(),
        "\"op\"".to_string(),
        "\"changed_at\"".to_string(),
        "\"commit_lsn\"".to_string(),
    ]);
    let mut select_columns = pk_columns.clone();
    select_columns.extend([
        "SNOWFLAKE_ID()".to_string(),
        "1".to_string(),
        "now()".to_string(),
        "pg_current_wal_lsn()".to_string(),
    ]);
    let order_direction = if ordering.ascending_oldest_first {
        "ASC"
    } else {
        "DESC"
    };
    let sql = format!(
        r#"
WITH candidate AS MATERIALIZED (
    SELECT {hot_pk_columns}, {order_column_ref} AS migration_order_value, hot.ctid AS hot_ctid
    FROM ONLY {table} AS hot
    LEFT JOIN {mirror} AS mirror
      ON {join_predicate}
    WHERE {mirror_missing_predicate}
    ORDER BY {order_column_ref} {order_direction}, hot.ctid ASC
    LIMIT $1
    FOR KEY SHARE OF hot SKIP LOCKED
),
initialized AS (
    INSERT INTO {mirror} ({insert_columns})
    SELECT {select_columns}
    FROM candidate
    ON CONFLICT ({conflict_columns}) DO NOTHING
    RETURNING 1
)
SELECT
    (SELECT count(*) FROM candidate) AS candidate_rows,
    (SELECT count(*) FROM initialized) AS initialized_rows
"#,
        hot_pk_columns = hot_pk_columns.join(", "),
        table = table.quoted(),
        mirror = mirror_table.quoted(),
        join_predicate = join_predicate,
        mirror_missing_predicate = mirror_missing_predicate,
        insert_columns = insert_columns.join(", "),
        select_columns = select_columns.join(", "),
        conflict_columns = pk_columns.join(", "),
    );
    let statement = SpiStatement::write("initialize change-log mirror batch", &sql)
        .map_err(|error| MirrorInitializationError::Spi(error.to_string()))?;

    Ok(MirrorInitializationBatchPlan {
        table: table.clone(),
        mirror_table: mirror_table.clone(),
        ordering,
        batch_size,
        statement,
    })
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
