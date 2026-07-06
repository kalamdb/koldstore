//! Change-log mirror planning for clean-schema managed tables.

use koldstore_core::{PrimaryKeyColumnShape, PrimaryKeyShape};
use thiserror::Error;

use crate::{spi::SpiStatement, sql::dml::MirrorCapturePlan};

use super::QualifiedTableName;

/// Schema that owns all clean-schema mirror tables.
pub const KOLDSTORE_SCHEMA: &str = "koldstore";
/// Suffix appended to the source table name for its latest-state mirror.
pub const CHANGE_LOG_MIRROR_SUFFIX: &str = "__cl";

/// Change-log mirror planning result.
pub type MirrorResult<T> = Result<T, MirrorError>;

/// Change-log mirror planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
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
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
    /// DML capture trigger planning failed.
    #[error("{0}")]
    Capture(String),
}

/// Planned change-log mirror artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeLogMirrorPlan {
    /// Source application table.
    pub source_table: QualifiedTableName,
    /// Generated mirror table in the koldstore schema.
    pub mirror_table: QualifiedTableName,
    /// Collision probe executed before creating the mirror.
    pub collision_probe: SpiStatement,
    /// Exact-PK mirror table DDL.
    pub create_table: SpiStatement,
    /// Sequence cursor index for flush and change-feed scans.
    pub seq_index: SpiStatement,
    /// Row-age policy index.
    pub changed_at_index: SpiStatement,
    /// Transactional DML capture function/triggers.
    pub capture: MirrorCapturePlan,
    /// Idempotent mirror drop used by rollback/demigration.
    pub drop_table: SpiStatement,
}

impl ChangeLogMirrorPlan {
    /// Statements required to create the mirror after collision checks pass.
    #[must_use]
    pub fn create_statements(&self) -> Vec<&SpiStatement> {
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
/// cannot be represented by the SPI boundary.
pub fn plan_change_log_mirror(
    source_table: &QualifiedTableName,
    primary_key: &PrimaryKeyShape,
) -> MirrorResult<ChangeLogMirrorPlan> {
    plan_change_log_mirror_from_columns(source_table, primary_key.columns())
}

/// Plans a per-table change-log mirror from ordered primary-key columns.
///
/// This helper exists so validation can reject an empty catalog result before
/// any mirror DDL is emitted.
///
/// # Errors
///
/// Returns an error when the key columns are empty, nullable, or statement
/// metadata cannot be prepared.
pub fn plan_change_log_mirror_from_columns(
    source_table: &QualifiedTableName,
    columns: &[PrimaryKeyColumnShape],
) -> MirrorResult<ChangeLogMirrorPlan> {
    if columns.is_empty() {
        return Err(MirrorError::MissingPrimaryKey);
    }
    for column in columns {
        if !column.not_null() {
            return Err(MirrorError::NullablePrimaryKey(
                column.column().as_str().to_string(),
            ));
        }
    }

    let mirror_table = mirror_relation_for_source(source_table)?;
    let quoted_mirror = mirror_table.quoted();
    let pk_columns = columns
        .iter()
        .map(|column| quote_ident(column.column().as_str()))
        .collect::<Vec<_>>();
    let mut ddl_columns = columns
        .iter()
        .map(render_pk_column)
        .collect::<MirrorResult<Vec<_>>>()?;
    ddl_columns.extend([
        "\"seq\" bigint NOT NULL".to_string(),
        "\"op\" smallint NOT NULL".to_string(),
        "\"changed_at\" timestamptz NOT NULL DEFAULT now()".to_string(),
        "\"commit_lsn\" pg_lsn NULL".to_string(),
        format!("PRIMARY KEY ({})", pk_columns.join(", ")),
    ]);

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {quoted_mirror} (\n    {}\n)",
        ddl_columns.join(",\n    ")
    );
    let seq_index_name = quote_ident(&format!("{}_seq_idx", mirror_table.name));
    let changed_at_index_name = quote_ident(&format!("{}_changed_at_idx", mirror_table.name));
    let collision_probe = SpiStatement::read(
        "check mirror table collision",
        &format!(
            "SELECT to_regclass('{}')::oid",
            sql_string_literal(&quoted_mirror)
        ),
    )
    .map_err(|error| MirrorError::Spi(error.to_string()))?;
    let create_table = SpiStatement::write("create change-log mirror table", &create_sql)
        .map_err(|error| MirrorError::Spi(error.to_string()))?;
    let seq_index = SpiStatement::write(
        "create change-log mirror seq index",
        &format!("CREATE INDEX IF NOT EXISTS {seq_index_name} ON {quoted_mirror} (\"seq\")"),
    )
    .map_err(|error| MirrorError::Spi(error.to_string()))?;
    let changed_at_index = SpiStatement::write(
        "create change-log mirror changed_at index",
        &format!(
            "CREATE INDEX IF NOT EXISTS {changed_at_index_name} ON {quoted_mirror} (\"changed_at\")"
        ),
    )
    .map_err(|error| MirrorError::Spi(error.to_string()))?;
    let drop_table = SpiStatement::write(
        "drop change-log mirror table",
        &format!("DROP TABLE IF EXISTS {quoted_mirror}"),
    )
    .map_err(|error| MirrorError::Spi(error.to_string()))?;
    let capture = crate::sql::dml::plan_mirror_capture(source_table, &mirror_table, columns)
        .map_err(|error| MirrorError::Capture(error.to_string()))?;

    Ok(ChangeLogMirrorPlan {
        source_table: source_table.clone(),
        mirror_table,
        collision_probe,
        create_table,
        seq_index,
        changed_at_index,
        capture,
        drop_table,
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
    let mirror_name = format!("{}{}", source_table.name, CHANGE_LOG_MIRROR_SUFFIX);
    if !is_safe_identifier(&mirror_name) {
        return Err(MirrorError::InvalidMirrorName(mirror_name));
    }

    Ok(QualifiedTableName {
        schema: Some(KOLDSTORE_SCHEMA.to_string()),
        name: mirror_name,
    })
}

fn render_pk_column(column: &PrimaryKeyColumnShape) -> MirrorResult<String> {
    let type_sql = render_type(column);
    let collation_sql = column
        .collation()
        .map(|collation| {
            format!(
                " COLLATE {}",
                quote_qualified_identifier(collation.as_str())
            )
        })
        .unwrap_or_default();

    Ok(format!(
        "{} {type_sql}{collation_sql} NOT NULL",
        quote_ident(column.column().as_str())
    ))
}

fn render_type(column: &PrimaryKeyColumnShape) -> String {
    if let Some(domain) = column.domain_identity() {
        return quote_qualified_identifier(domain.as_str());
    }

    let type_name = column.type_name().as_str();
    match (type_name, column.typmod().get()) {
        ("character varying" | "varchar", typmod) if typmod >= 4 => {
            format!("varchar({})", typmod - 4)
        }
        ("character" | "bpchar", typmod) if typmod >= 4 => {
            format!("character({})", typmod - 4)
        }
        ("numeric", typmod) if typmod >= 4 => {
            let packed = typmod - 4;
            let precision = (packed >> 16) & 0xffff;
            let scale = packed & 0xffff;
            format!("numeric({precision},{scale})")
        }
        ("character varying", _) => "varchar".to_string(),
        ("timestamp with time zone", _) => "timestamptz".to_string(),
        ("timestamp without time zone", _) => "timestamp".to_string(),
        ("time with time zone", _) => "timetz".to_string(),
        ("time without time zone", _) => "time".to_string(),
        (plain, _) if is_safe_identifier(plain) => plain.to_string(),
        (qualified, _) => quote_qualified_identifier(qualified),
    }
}

fn quote_qualified_identifier(value: &str) -> String {
    value
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
