//! Mirror table schema planning.

use koldstore_common::{
    escape_sql_literal, is_safe_identifier, quote_ident, quote_qualified_ident,
    PrimaryKeyColumnShape, SqlStatement,
};

use super::columns::MirrorColumn;
use super::error::{MirrorError, MirrorResult};
use super::relation::{bounded_identifier, legacy_truncated_identifier, MirrorRelation};

const SEQ_INDEX_SUFFIX: &str = "_seq_idx";
const TOMBSTONE_INDEX_SUFFIX: &str = "_tombstone_seq_idx";

/// Primitive mirror table schema statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorSchemaPlan {
    /// Collision probe executed before creating the mirror.
    pub collision_probe: SqlStatement,
    /// Exact-PK mirror table DDL.
    pub create_table: SqlStatement,
    /// Sequence cursor index for scans.
    pub seq_index: SqlStatement,
    /// Partial index over delete-marker rows, keyed by `seq`.
    ///
    /// PERFORMANCE: keeps force-flush tombstone-only selection (stats +
    /// mirror-op-filtered fetch) index-backed instead of scanning every live
    /// mirror row to find the handful of pending deletes.
    pub tombstone_index: SqlStatement,
    /// Idempotent mirror drop.
    pub drop_table: SqlStatement,
}

impl MirrorSchemaPlan {
    /// Statements required to create mirror storage after collision checks pass.
    #[must_use]
    pub fn create_statements(&self) -> [&SqlStatement; 3] {
        [&self.create_table, &self.seq_index, &self.tombstone_index]
    }
}

/// Plans `ALTER TABLE … RENAME COLUMN` statements for mirror primary-key columns.
///
/// Used after a managed source table renames PK attributes so the `__cl` mirror
/// storage column names stay aligned with capture/apply SQL.
///
/// # Errors
///
/// Returns an error when a rename uses an unsafe identifier or statement
/// metadata is invalid.
pub fn plan_mirror_pk_column_renames(
    mirror_table: &MirrorRelation,
    renames: &[(String, String)],
) -> MirrorResult<Vec<SqlStatement>> {
    let quoted_mirror = mirror_table.quoted();
    let mut statements = Vec::with_capacity(renames.len());
    for (old_name, new_name) in renames {
        if old_name == new_name {
            continue;
        }
        if !is_safe_identifier(old_name) || !is_safe_identifier(new_name) {
            return Err(MirrorError::InvalidColumn(format!(
                "{old_name} -> {new_name}"
            )));
        }
        statements.push(SqlStatement::write(
            "rename change-log mirror primary-key column",
            &format!(
                "ALTER TABLE {quoted_mirror} RENAME COLUMN {} TO {}",
                quote_ident(old_name),
                quote_ident(new_name)
            ),
        )?);
    }
    Ok(statements)
}

/// Plans the DDL required to rename a mirror and its generated indexes.
///
/// The legacy index lookup keeps mirrors created before bounded index naming
/// movable. PostgreSQL silently truncated those names at creation time.
///
/// # Errors
///
/// Returns an error when any generated statement metadata is invalid.
pub fn plan_mirror_relation_rename(
    old_mirror: &MirrorRelation,
    new_mirror: &MirrorRelation,
) -> MirrorResult<Vec<SqlStatement>> {
    if old_mirror == new_mirror {
        return Ok(Vec::new());
    }

    let old_relation = old_mirror.relation();
    let new_relation = new_mirror.relation();
    let mut statements = Vec::with_capacity(5);
    statements.push(SqlStatement::write(
        "rename change-log mirror table",
        &format!(
            "ALTER TABLE {} RENAME TO {}",
            old_mirror.quoted(),
            quote_ident(new_relation)
        ),
    )?);
    for suffix in [SEQ_INDEX_SUFFIX, TOMBSTONE_INDEX_SUFFIX] {
        let old_legacy_name = legacy_truncated_identifier(old_relation, suffix);
        let old_bounded_name = bounded_identifier(old_relation, suffix);
        let new_name = bounded_identifier(new_relation, suffix);
        statements.push(rename_index_if_present(&old_legacy_name, &new_name)?);
        if old_bounded_name != old_legacy_name {
            statements.push(rename_index_if_present(&old_bounded_name, &new_name)?);
        }
    }
    Ok(statements)
}

/// Plans primitive mirror table storage statements.
///
/// # Errors
///
/// Returns an error when the key shape is empty, contains nullable columns, or
/// statement metadata is invalid.
pub fn plan_mirror_schema(
    mirror_table: &MirrorRelation,
    primary_key: &[PrimaryKeyColumnShape],
) -> MirrorResult<MirrorSchemaPlan> {
    plan_mirror_schema_with_order_key(mirror_table, primary_key, false)
}

/// Plans primitive mirror storage with an optional encoded segment-order key.
///
/// # Errors
///
/// Returns an error when the key shape is empty, contains nullable columns, or
/// statement metadata is invalid.
pub fn plan_mirror_schema_with_order_key(
    mirror_table: &MirrorRelation,
    primary_key: &[PrimaryKeyColumnShape],
    include_order_key: bool,
) -> MirrorResult<MirrorSchemaPlan> {
    if primary_key.is_empty() {
        return Err(MirrorError::MissingPrimaryKey);
    }
    for column in primary_key {
        if !column.not_null() {
            return Err(MirrorError::NullablePrimaryKey(
                column.column().as_str().to_string(),
            ));
        }
    }

    let quoted_mirror = mirror_table.quoted();
    let pk_columns = primary_key
        .iter()
        .map(|column| quote_ident(column.column().as_str()))
        .collect::<Vec<_>>();
    let mut ddl_columns = primary_key
        .iter()
        .map(render_pk_column)
        .collect::<MirrorResult<Vec<_>>>()?;
    if include_order_key {
        ddl_columns.push("\"order_key\" bytea NOT NULL".to_string());
    }
    ddl_columns.extend([
        MirrorColumn::Seq.definition().to_string(),
        MirrorColumn::Op.definition().to_string(),
        format!("PRIMARY KEY ({})", pk_columns.join(", ")),
    ]);

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {quoted_mirror} (\n    {}\n)",
        ddl_columns.join(",\n    ")
    );
    let seq_index_name = quote_ident(&bounded_identifier(
        mirror_table.relation(),
        SEQ_INDEX_SUFFIX,
    ));
    let tombstone_index_name = quote_ident(&bounded_identifier(
        mirror_table.relation(),
        TOMBSTONE_INDEX_SUFFIX,
    ));

    Ok(MirrorSchemaPlan {
        collision_probe: SqlStatement::read(
            "check mirror table collision",
            &format!(
                "SELECT to_regclass('{}')::oid",
                escape_sql_literal(&quoted_mirror)
            ),
        )?,
        create_table: SqlStatement::write("create change-log mirror table", &create_sql)?,
        seq_index: SqlStatement::write(
            "create change-log mirror seq index",
            &format!("CREATE INDEX IF NOT EXISTS {seq_index_name} ON {quoted_mirror} (\"seq\")"),
        )?,
        tombstone_index: SqlStatement::write(
            "create change-log mirror tombstone index",
            &format!(
                "CREATE INDEX IF NOT EXISTS {tombstone_index_name} ON {quoted_mirror} (\"seq\") WHERE \"op\" = 3"
            ),
        )?,
        drop_table: plan_drop_mirror_table(mirror_table)?,
    })
}

fn rename_index_if_present(old_name: &str, new_name: &str) -> MirrorResult<SqlStatement> {
    Ok(SqlStatement::write(
        "rename change-log mirror index",
        &format!(
            "ALTER INDEX IF EXISTS {}.{} RENAME TO {}",
            quote_ident(super::relation::KOLDSTORE_SCHEMA),
            quote_ident(old_name),
            quote_ident(new_name)
        ),
    )?)
}

/// Plans idempotent mirror table drop.
///
/// # Errors
///
/// Returns an error when statement metadata is invalid.
pub fn plan_drop_mirror_table(mirror_table: &MirrorRelation) -> MirrorResult<SqlStatement> {
    Ok(SqlStatement::write(
        "drop change-log mirror table",
        &format!("DROP TABLE IF EXISTS {}", mirror_table.quoted()),
    )?)
}

fn render_pk_column(column: &PrimaryKeyColumnShape) -> MirrorResult<String> {
    let type_sql = render_type(column);
    let collation_sql = column
        .collation()
        .map(|collation| format!(" COLLATE {}", quote_qualified_ident(collation.as_str())))
        .unwrap_or_default();

    Ok(format!(
        "{} {type_sql}{collation_sql} NOT NULL",
        quote_ident(column.column().as_str())
    ))
}

fn render_type(column: &PrimaryKeyColumnShape) -> String {
    if let Some(domain) = column.domain_identity() {
        return quote_qualified_ident(domain.as_str());
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
        (qualified, _) => quote_qualified_ident(qualified),
    }
}
