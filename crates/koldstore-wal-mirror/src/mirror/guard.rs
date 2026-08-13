//! Primary-key / segment-order mutation guard for managed source tables.
//!
//! Logical decoding does not enforce immutability of published identity
//! columns. These triggers reject PK or segment-order column updates on the
//! source heap so mirror and cold identity stay stable.

use koldstore_common::{quote_ident, PrimaryKeyColumnShape, QualifiedTableName, SqlStatement};
use thiserror::Error;

use super::shared::relation::{bounded_identifier, legacy_truncated_identifier};

const PK_GUARD_FUNCTION_SUFFIX: &str = "_pk_guard";
const PK_UPDATE_GUARD_TRIGGER_SUFFIX: &str = "_pk_update_guard";

/// PK-guard planning result.
pub type MirrorGuardResult<T> = Result<T, MirrorGuardError>;

/// PK-guard planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MirrorGuardError {
    /// Guard triggers require at least one primary-key column.
    #[error("mirror primary-key guard requires at least one primary-key column")]
    MissingPrimaryKey,
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// Planned primary-key mutation guard artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorPkGuardPlan {
    /// Function that rejects an actual primary-key / order-column change.
    pub function: SqlStatement,
    /// Column-specific row trigger; ordinary non-PK updates never invoke it.
    pub trigger: SqlStatement,
    /// Idempotent guard-trigger cleanup statement.
    pub drop_trigger: SqlStatement,
    /// Idempotent guard-function cleanup statement.
    pub drop_function: SqlStatement,
}

impl MirrorPkGuardPlan {
    /// Create statements in dependency order.
    #[must_use]
    pub fn create_statements(&self) -> [&SqlStatement; 2] {
        [&self.function, &self.trigger]
    }
}

/// Plans the PK/order-column mutation guard for a managed source table.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied or statement
/// metadata cannot be represented.
pub fn plan_mirror_pk_guard(
    source_table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
    order_column: Option<&str>,
) -> MirrorGuardResult<MirrorPkGuardPlan> {
    if primary_key.is_empty() {
        return Err(MirrorGuardError::MissingPrimaryKey);
    }

    let guard_function_name = pk_guard_function_relation(&mirror_table.name);
    let source = source_table.quoted();
    let function = SqlStatement::write(
        "create change-log mirror primary-key guard function",
        &pk_guard_function_sql(&guard_function_name, primary_key, order_column),
    )
    .map_err(|error| MirrorGuardError::Sql(error.to_string()))?;
    let trigger = plan_pk_guard_trigger(
        mirror_table,
        &source,
        &guard_function_name,
        primary_key,
        order_column,
    )?;
    let drop_trigger = SqlStatement::write(
        "drop change-log mirror primary-key guard trigger",
        &drop_trigger_if_present_sql(&pk_guard_trigger_name(&mirror_table.name), &source),
    )
    .map_err(|error| MirrorGuardError::Sql(error.to_string()))?;
    let drop_function = SqlStatement::write(
        "drop change-log mirror primary-key guard function",
        &format!("DROP FUNCTION IF EXISTS {}()", guard_function_name.quoted()),
    )
    .map_err(|error| MirrorGuardError::Sql(error.to_string()))?;

    Ok(MirrorPkGuardPlan {
        function,
        trigger,
        drop_trigger,
        drop_function,
    })
}

/// Plans idempotent teardown of PK guard triggers/functions and any leftover
/// legacy capture triggers from pre-WAL-only installs.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_mirror_source_teardown(
    source_table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
) -> koldstore_common::SqlResult<Vec<SqlStatement>> {
    use koldstore_common::MirrorOperation;

    let function_name = QualifiedTableName {
        schema: Some("koldstore".to_string()),
        name: format!("{}_capture", mirror_table.name),
    };
    let guard_function_name = pk_guard_function_relation(&mirror_table.name);
    let legacy_guard_function_name = QualifiedTableName {
        schema: Some("koldstore".to_string()),
        name: legacy_truncated_identifier(&mirror_table.name, PK_GUARD_FUNCTION_SUFFIX),
    };
    let source = source_table.quoted();
    let mut statements = Vec::with_capacity(8);
    for operation in MirrorOperation::ALL {
        let trigger_name = operation.capture_trigger_name(&mirror_table.name);
        statements.push(SqlStatement::write(
            &format!(
                "drop change-log mirror {} capture trigger",
                operation.capture_trigger_suffix()
            ),
            &drop_trigger_if_present_sql(&trigger_name, &source),
        )?);
    }
    let bounded_trigger = pk_guard_trigger_name(&mirror_table.name);
    let legacy_trigger =
        legacy_truncated_identifier(&mirror_table.name, PK_UPDATE_GUARD_TRIGGER_SUFFIX);
    statements.push(SqlStatement::write(
        "drop change-log mirror primary-key guard trigger",
        &drop_trigger_if_present_sql(&bounded_trigger, &source),
    )?);
    if legacy_trigger != bounded_trigger {
        statements.push(SqlStatement::write(
            "drop legacy change-log mirror primary-key guard trigger",
            &drop_trigger_if_present_sql(&legacy_trigger, &source),
        )?);
    }
    statements.push(SqlStatement::write(
        "drop change-log mirror capture function",
        &format!("DROP FUNCTION IF EXISTS {}()", function_name.quoted()),
    )?);
    statements.push(SqlStatement::write(
        "drop change-log mirror primary-key guard function",
        &format!("DROP FUNCTION IF EXISTS {}()", guard_function_name.quoted()),
    )?);
    if legacy_guard_function_name.name != guard_function_name.name {
        statements.push(SqlStatement::write(
            "drop legacy change-log mirror primary-key guard function",
            &format!(
                "DROP FUNCTION IF EXISTS {}()",
                legacy_guard_function_name.quoted()
            ),
        )?);
    }
    Ok(statements)
}

/// Builds the PostgreSQL-safe PK-update guard trigger name for a mirror table.
#[must_use]
pub fn pk_guard_trigger_name(mirror_table_name: &str) -> String {
    bounded_identifier(mirror_table_name, PK_UPDATE_GUARD_TRIGGER_SUFFIX)
}

fn pk_guard_function_relation(mirror_table_name: &str) -> QualifiedTableName {
    QualifiedTableName {
        schema: Some("koldstore".to_string()),
        name: bounded_identifier(mirror_table_name, PK_GUARD_FUNCTION_SUFFIX),
    }
}

/// Drops a trigger only when it already exists, without PostgreSQL's
/// `DROP TRIGGER IF EXISTS` NOTICE on a missing name (common on first manage).
fn drop_trigger_if_present_sql(trigger_name: &str, source_table: &str) -> String {
    let quoted_trigger = quote_ident(trigger_name);
    format!(
        r#"DO $koldstore_drop_trigger$
BEGIN
  BEGIN
    EXECUTE 'DROP TRIGGER {quoted_trigger} ON {source_table}';
  EXCEPTION WHEN undefined_object THEN
    NULL;
  END;
END
$koldstore_drop_trigger$;"#
    )
}

fn pk_guard_function_sql(
    function_name: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
    order_column: Option<&str>,
) -> String {
    let mut distinct = primary_key
        .iter()
        .map(|column| {
            let name = quote_ident(column.column().as_str());
            format!("OLD.{name} IS DISTINCT FROM NEW.{name}")
        })
        .collect::<Vec<_>>();
    if let Some(order_column) = order_column {
        let name = quote_ident(order_column);
        let predicate = format!("OLD.{name} IS DISTINCT FROM NEW.{name}");
        if !distinct.contains(&predicate) {
            distinct.push(predicate);
        }
    }
    let distinct = distinct.join("\n       OR ");

    format!(
        r#"
CREATE OR REPLACE FUNCTION {function_name}()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, koldstore
AS $$
BEGIN
    IF {distinct} THEN
        RAISE EXCEPTION
            'pg-koldstore does not support primary-key or segment-order-column updates on managed table %',
            TG_TABLE_NAME;
    END IF;
    RETURN NEW;
END;
$$
"#,
        function_name = function_name.quoted(),
    )
}

fn plan_pk_guard_trigger(
    mirror_table: &QualifiedTableName,
    source_table: &str,
    function_name: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
    order_column: Option<&str>,
) -> MirrorGuardResult<SqlStatement> {
    let trigger_name = pk_guard_trigger_name(&mirror_table.name);
    let mut of_columns = primary_key
        .iter()
        .map(|column| quote_ident(column.column().as_str()))
        .collect::<Vec<_>>();
    if let Some(order_column) = order_column {
        let name = quote_ident(order_column);
        if !of_columns.contains(&name) {
            of_columns.push(name);
        }
    }
    let of_list = of_columns.join(", ");
    let drop_sql = drop_trigger_if_present_sql(&trigger_name, source_table);
    SqlStatement::write(
        "create change-log mirror primary-key guard trigger",
        &format!(
            r#"
{drop_sql}
CREATE TRIGGER {trigger_name}
BEFORE UPDATE OF {of_list} ON {source_table}
FOR EACH ROW EXECUTE FUNCTION {function_name}()
"#,
            trigger_name = quote_ident(&trigger_name),
            function_name = function_name.quoted(),
        ),
    )
    .map_err(|error| MirrorGuardError::Sql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use koldstore_common::{
        ColumnId, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
    };

    fn pk_column(name: &str) -> PrimaryKeyColumnShape {
        PrimaryKeyColumnShape::new(
            ColumnId::from_attnum(1),
            PkColumn::new(name).unwrap(),
            PkOrdinal::new(1).unwrap(),
            PgTypeOid::new(20).unwrap(),
            PgTypeName::new("bigint").unwrap(),
            PgTypmod::new(-1),
            None,
            None,
            true,
        )
    }

    #[test]
    fn short_mirror_names_keep_readable_guard_suffixes() {
        assert_eq!(
            pk_guard_trigger_name("public_messages__cl"),
            "public_messages__cl_pk_update_guard"
        );
        assert_eq!(
            pk_guard_function_relation("public_messages__cl").name,
            "public_messages__cl_pk_guard"
        );
    }

    #[test]
    fn long_mirror_names_keep_guard_identifiers_within_postgres_limit() {
        let mirror = format!("{}__cl", "a".repeat(59));
        assert_eq!(mirror.len(), 63);

        let trigger = pk_guard_trigger_name(&mirror);
        let function = pk_guard_function_relation(&mirror).name;
        assert!(trigger.len() <= 63, "trigger={trigger}");
        assert!(function.len() <= 63, "function={function}");
        assert!(trigger.ends_with(PK_UPDATE_GUARD_TRIGGER_SUFFIX));
        assert!(function.ends_with(PK_GUARD_FUNCTION_SUFFIX));
        assert_ne!(trigger, mirror);

        let source = QualifiedTableName::parse("public.messages").unwrap();
        let mirror_table = QualifiedTableName::parse(&format!("koldstore.{mirror}")).unwrap();
        let plan = plan_mirror_pk_guard(&source, &mirror_table, &[pk_column("id")], None).unwrap();
        assert!(plan
            .trigger
            .sql
            .contains(&format!("CREATE TRIGGER \"{trigger}\"")));
        assert!(plan.function.sql.contains(&format!(
            "CREATE OR REPLACE FUNCTION \"koldstore\".\"{function}\"()"
        )));
    }
}
