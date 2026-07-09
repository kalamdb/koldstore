//! Hot cleanup after manifest commit.

use koldstore_mirror::{
    mirror_delete_using_selected_sql, quoted_pk_columns, selected_record_columns, MirrorRelation,
};
use thiserror::Error;

use koldstore_common::{quote_ident, QualifiedTableName, SqlParamType, SqlStatement};

/// Clean-schema cleanup planning result.
pub type CleanupResult<T> = Result<T, CleanupError>;

/// Clean-schema cleanup planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CleanupError {
    /// Primary key is needed to fence cleanup.
    #[error("clean-schema cleanup requires at least one primary-key column")]
    MissingPrimaryKey,
    /// Identifier is unsafe to quote.
    #[error("invalid cleanup identifier `{0}`")]
    InvalidIdentifier(String),
    /// SPI statement metadata could not be prepared.
    #[error("{0}")]
    Spi(String),
}

impl From<koldstore_mirror::MirrorError> for CleanupError {
    fn from(error: koldstore_mirror::MirrorError) -> Self {
        match error {
            koldstore_mirror::MirrorError::MissingPrimaryKey => Self::MissingPrimaryKey,
            koldstore_mirror::MirrorError::InvalidColumn(name) => Self::InvalidIdentifier(name),
            other => Self::Spi(other.to_string()),
        }
    }
}

/// Planned clean-schema cleanup statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanSchemaCleanupPlan {
    /// Source user table.
    pub table: QualifiedTableName,
    /// Table-specific mirror table.
    pub mirror_table: QualifiedTableName,
    /// Parameterized cleanup statement. `$1` is a JSON selected-set.
    pub statement: SqlStatement,
}

/// Planned hot cleanup behavior after a flush attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HotCleanupPlan {
    /// Whether live hot rows may be removed.
    pub remove_live_hot_rows: bool,
    /// Whether a hot tombstone must remain to mask older cold rows.
    pub retain_tombstone: bool,
}

/// Returns whether cleanup may remove live hot rows.
#[must_use]
pub fn cleanup_allowed(manifest_committed: bool) -> bool {
    manifest_committed
}

/// Returns whether a tombstone should be retained after cleanup.
#[must_use]
pub const fn retain_tombstone(cold_may_contain_pk: bool) -> bool {
    cold_may_contain_pk
}

/// Plans hot cleanup after manifest commit.
#[must_use]
pub const fn plan_hot_cleanup(
    manifest_committed: bool,
    cold_may_contain_pk: bool,
) -> HotCleanupPlan {
    HotCleanupPlan {
        remove_live_hot_rows: manifest_committed,
        retain_tombstone: retain_tombstone(cold_may_contain_pk),
    }
}

/// Catalog column metadata needed for typed cleanup record decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupCatalogColumn {
    /// Column name.
    pub name: String,
    /// Original `format_type` spelling for `jsonb_to_recordset`.
    pub catalog_type_name: String,
}

/// Plans cleanup for a committed clean-schema flush selected set.
///
/// `$1::jsonb` is expected to be an array of objects containing primary-key
/// columns plus `seq` and `op`. Cleanup removes mirror rows first, then matching
/// live base rows, in one atomic SQL statement so the hot heap and `__cl` mirror
/// cannot diverge if the statement succeeds.
///
/// # Errors
///
/// Returns an error when the primary key is empty, identifiers are unsafe, or
/// statement metadata cannot be prepared.
pub fn plan_clean_schema_cleanup(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
) -> CleanupResult<CleanSchemaCleanupPlan> {
    let primary_key: Vec<&str> = primary_key_columns.iter().map(String::as_str).collect();
    let record_columns = selected_record_columns(&primary_key)?;
    plan_cleanup_statement(
        table,
        mirror_table,
        primary_key_columns,
        &record_columns,
        CleanupPkCoercion::Text,
        "clean schema flush cleanup",
    )
}

/// Plans typed cleanup using catalog column types for primary-key fields.
///
/// Prefer this over [`plan_clean_schema_cleanup`] when catalog types are known,
/// so `jsonb_to_recordset` decodes PK values without lossy text coercion.
///
/// # Errors
///
/// Returns an error when the primary key is empty, a PK column is missing from
/// the catalog, identifiers are unsafe, or statement metadata cannot be prepared.
pub fn plan_typed_clean_schema_cleanup(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    catalog_columns: &[CleanupCatalogColumn],
) -> CleanupResult<CleanSchemaCleanupPlan> {
    let record_columns = typed_selected_record_columns(primary_key_columns, catalog_columns)?;
    plan_cleanup_statement(
        table,
        mirror_table,
        primary_key_columns,
        &record_columns,
        CleanupPkCoercion::Native,
        "typed clean schema flush cleanup",
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupPkCoercion {
    /// Compare PK values as text (legacy / test path).
    Text,
    /// Compare PK values with native catalog types.
    Native,
}

fn plan_cleanup_statement(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    record_columns: &str,
    coercion: CleanupPkCoercion,
    operation: &str,
) -> CleanupResult<CleanSchemaCleanupPlan> {
    if primary_key_columns.is_empty() {
        return Err(CleanupError::MissingPrimaryKey);
    }

    let primary_key: Vec<&str> = primary_key_columns.iter().map(String::as_str).collect();
    let mirror = mirror_table
        .as_table_name()
        .map(MirrorRelation::new)
        .map_err(|error| CleanupError::Spi(error.to_string()))?;
    let pk_columns = quoted_pk_columns(&primary_key)?;
    let mirror_delete = match coercion {
        CleanupPkCoercion::Text => mirror_delete_using_selected_sql(&mirror, &primary_key)?,
        CleanupPkCoercion::Native => {
            let mirror_join = pk_columns
                .iter()
                .map(|column| format!("mirror.{column} = selected.{column}"))
                .chain(std::iter::once(
                    "mirror.\"seq\" = selected.\"seq\"".to_string(),
                ))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!(
                "DELETE FROM {mirror} AS mirror\n    USING selected\n    WHERE {mirror_join}",
                mirror = mirror.quoted()
            )
        }
    };
    let hot_join = pk_columns
        .iter()
        .map(|column| match coercion {
            CleanupPkCoercion::Text => format!("hot.{column}::text = selected.{column}"),
            CleanupPkCoercion::Native => format!("hot.{column} = selected.{column}"),
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        r#"
WITH selected AS (
    SELECT *
    FROM jsonb_to_recordset($1::jsonb) AS selected({record_columns})
),
removed_mirror AS (
    {mirror_delete}
    RETURNING mirror."seq"
),
deleted_hot AS (
    DELETE FROM ONLY {table} AS hot
    USING selected, removed_mirror
    WHERE selected."op" IN (1, 2)
      AND removed_mirror."seq" = selected."seq"
      AND {hot_join}
    RETURNING 1
)
SELECT
    (SELECT count(*)::bigint FROM removed_mirror) AS mirror_pruned,
    (SELECT count(*)::bigint FROM deleted_hot) AS hot_pruned
"#,
        table = table.quoted(),
    );
    let statement = SqlStatement::write_with_params(operation, &sql, [SqlParamType::Jsonb])
        .map_err(|error| CleanupError::Spi(error.to_string()))?;

    Ok(CleanSchemaCleanupPlan {
        table: table.clone(),
        mirror_table: mirror_table.clone(),
        statement,
    })
}

fn typed_selected_record_columns(
    primary_key_columns: &[String],
    catalog_columns: &[CleanupCatalogColumn],
) -> CleanupResult<String> {
    let mut record_columns = Vec::with_capacity(primary_key_columns.len() + 2);
    for primary_key in primary_key_columns {
        let column = catalog_columns
            .iter()
            .find(|column| column.name == *primary_key)
            .ok_or_else(|| CleanupError::InvalidIdentifier(primary_key.clone()))?;
        record_columns.push(format!(
            "{} {}",
            quote_ident(primary_key),
            column.catalog_type_name
        ));
    }
    record_columns.push("\"seq\" bigint".to_string());
    record_columns.push("\"op\" smallint".to_string());
    Ok(record_columns.join(", "))
}

/// Plans cleanup for a contiguous oldest-by-`seq` flush without per-row JSON.
///
/// PERFORMANCE: Policy and force flushes select a seq prefix (`seq <= max_seq`).
/// Cleanup can delete that prefix directly from the mirror (optionally filtered
/// by mirror op codes) and join hot deletes from the removed set — no
/// `jsonb_to_recordset` materialization of every flushed PK.
///
/// Bind parameters:
/// - `$1` inclusive mirror `seq` upper bound
///
/// # Errors
///
/// Returns an error when the primary key is empty, identifiers are unsafe, or
/// statement metadata cannot be prepared.
pub fn plan_seq_range_cleanup(
    table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key_columns: &[String],
    mirror_ops: Option<&[i16]>,
) -> CleanupResult<CleanSchemaCleanupPlan> {
    if primary_key_columns.is_empty() {
        return Err(CleanupError::MissingPrimaryKey);
    }

    let primary_key: Vec<&str> = primary_key_columns.iter().map(String::as_str).collect();
    let mirror = mirror_table
        .as_table_name()
        .map(MirrorRelation::new)
        .map_err(|error| CleanupError::Spi(error.to_string()))?;
    let pk_columns = quoted_pk_columns(&primary_key)?;
    let returning_columns = pk_columns
        .iter()
        .map(|column| format!("mirror.{column}"))
        .chain(["mirror.\"seq\"".to_string(), "mirror.\"op\"".to_string()])
        .collect::<Vec<_>>()
        .join(", ");
    let mut mirror_where = vec!["mirror.\"seq\" <= $1::bigint".to_string()];
    if let Some(ops) = mirror_ops {
        if !ops.is_empty() {
            if ops.len() == 1 {
                mirror_where.push(format!("mirror.\"op\" = {}", ops[0]));
            } else {
                let literals = ops
                    .iter()
                    .map(i16::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                mirror_where.push(format!("mirror.\"op\" IN ({literals})"));
            }
        }
    }
    let hot_join = pk_columns
        .iter()
        .map(|column| format!("hot.{column} = removed_mirror.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        r#"
WITH removed_mirror AS (
    DELETE FROM {mirror} AS mirror
    WHERE {mirror_where}
    RETURNING {returning_columns}
),
deleted_hot AS (
    DELETE FROM ONLY {table} AS hot
    USING removed_mirror
    WHERE removed_mirror."op" IN (1, 2)
      AND {hot_join}
    RETURNING 1
)
SELECT
    (SELECT count(*)::bigint FROM removed_mirror) AS mirror_pruned,
    (SELECT count(*)::bigint FROM deleted_hot) AS hot_pruned
"#,
        mirror = mirror.quoted(),
        mirror_where = mirror_where.join(" AND "),
        returning_columns = returning_columns,
        table = table.quoted(),
        hot_join = hot_join,
    );
    let statement = SqlStatement::write_with_params(
        "seq-range clean schema flush cleanup",
        &sql,
        [SqlParamType::BigInt],
    )
    .map_err(|error| CleanupError::Spi(error.to_string()))?;

    Ok(CleanSchemaCleanupPlan {
        table: table.clone(),
        mirror_table: mirror_table.clone(),
        statement,
    })
}
