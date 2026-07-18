//! Mirror table schema planning.

use koldstore_common::{
    escape_sql_literal, is_safe_identifier, quote_ident, quote_qualified_ident,
    PrimaryKeyColumnShape,
};

use super::columns::MirrorColumn;
use super::error::{MirrorError, MirrorResult};
use super::relation::MirrorRelation;
use super::statement::MirrorStatement;

/// Primitive mirror table schema statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorSchemaPlan {
    /// Collision probe executed before creating the mirror.
    pub collision_probe: MirrorStatement,
    /// Exact-PK mirror table DDL.
    pub create_table: MirrorStatement,
    /// Drops legacy `commit_lsn` from mirrors created before the slim schema.
    pub drop_legacy_commit_lsn: MirrorStatement,
    /// Sequence cursor index for scans.
    pub seq_index: MirrorStatement,
    /// Partial index over delete-marker rows, keyed by `seq`.
    ///
    /// PERFORMANCE: keeps force-flush tombstone-only selection (stats +
    /// mirror-op-filtered fetch) index-backed instead of scanning every live
    /// mirror row to find the handful of pending deletes.
    pub tombstone_index: MirrorStatement,
    /// Idempotent mirror drop.
    pub drop_table: MirrorStatement,
}

impl MirrorSchemaPlan {
    /// Statements required to create mirror storage after collision checks pass.
    #[must_use]
    pub fn create_statements(&self) -> [&MirrorStatement; 4] {
        [
            &self.create_table,
            &self.drop_legacy_commit_lsn,
            &self.seq_index,
            &self.tombstone_index,
        ]
    }
}

/// Plans primitive mirror table storage statements.
///
/// # Errors
///
/// Returns an error when the key shape is empty or contains nullable columns.
pub fn plan_mirror_schema(
    mirror_table: &MirrorRelation,
    primary_key: &[PrimaryKeyColumnShape],
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
    ddl_columns.extend([
        MirrorColumn::Seq.definition().to_string(),
        MirrorColumn::Op.definition().to_string(),
        format!("PRIMARY KEY ({})", pk_columns.join(", ")),
    ]);

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {quoted_mirror} (\n    {}\n)",
        ddl_columns.join(",\n    ")
    );
    let seq_index_name = quote_ident(&format!("{}_seq_idx", mirror_table.relation()));
    let tombstone_index_name =
        quote_ident(&format!("{}_tombstone_seq_idx", mirror_table.relation()));

    Ok(MirrorSchemaPlan {
        collision_probe: MirrorStatement::read(
            "check mirror table collision",
            format!(
                "SELECT to_regclass('{}')::oid",
                escape_sql_literal(&quoted_mirror)
            ),
        ),
        create_table: MirrorStatement::write("create change-log mirror table", create_sql),
        drop_legacy_commit_lsn: MirrorStatement::write(
            "drop legacy commit_lsn mirror column",
            format!("ALTER TABLE {quoted_mirror} DROP COLUMN IF EXISTS \"commit_lsn\""),
        ),
        seq_index: MirrorStatement::write(
            "create change-log mirror seq index",
            format!("CREATE INDEX IF NOT EXISTS {seq_index_name} ON {quoted_mirror} (\"seq\")"),
        ),
        tombstone_index: MirrorStatement::write(
            "create change-log mirror tombstone index",
            format!(
                "CREATE INDEX IF NOT EXISTS {tombstone_index_name} ON {quoted_mirror} (\"seq\") WHERE \"op\" = 3"
            ),
        ),
        drop_table: plan_drop_mirror_table(mirror_table),
    })
}

/// Plans idempotent mirror table drop.
#[must_use]
pub fn plan_drop_mirror_table(mirror_table: &MirrorRelation) -> MirrorStatement {
    MirrorStatement::write(
        "drop change-log mirror table",
        format!("DROP TABLE IF EXISTS {}", mirror_table.quoted()),
    )
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
