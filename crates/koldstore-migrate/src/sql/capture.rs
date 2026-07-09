//! Change-log mirror capture SQL plans.

use koldstore_common::{
    quote_ident, session::snowflake_id_call_expression, MirrorOperation, PrimaryKeyColumnShape,
    SqlStatement,
};
use koldstore_mirror::{plan_upsert_mirror_row, MirrorRelation};
use thiserror::Error;

use crate::QualifiedTableName;

/// Change-log mirror DML capture planning result.
pub type MirrorCaptureResult<T> = Result<T, MirrorCaptureError>;

/// Change-log mirror DML capture planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MirrorCaptureError {
    /// A capture trigger cannot be generated without primary-key columns.
    #[error("mirror capture requires at least one primary-key column")]
    MissingPrimaryKey,
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// Planned mirror DML capture artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorCapturePlan {
    /// Trigger function that upserts latest-state rows into the mirror.
    pub function: SqlStatement,
    /// INSERT capture trigger.
    pub insert_trigger: SqlStatement,
    /// UPDATE capture trigger.
    pub update_trigger: SqlStatement,
    /// DELETE capture trigger.
    pub delete_trigger: SqlStatement,
    /// Idempotent trigger cleanup statement.
    pub drop_triggers: SqlStatement,
    /// Idempotent trigger function cleanup statement.
    pub drop_function: SqlStatement,
}

impl MirrorCapturePlan {
    /// Trigger creation statements in dependency order.
    #[must_use]
    pub fn trigger_statements(&self) -> [&SqlStatement; 3] {
        [
            &self.insert_trigger,
            &self.update_trigger,
            &self.delete_trigger,
        ]
    }

    /// All create statements in dependency order.
    #[must_use]
    pub fn create_statements(&self) -> [&SqlStatement; 4] {
        [
            &self.function,
            &self.insert_trigger,
            &self.update_trigger,
            &self.delete_trigger,
        ]
    }
}

/// Plans transactional DML capture from a source table into its latest-state mirror.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied or statement
/// metadata cannot be represented by the SQL helper.
pub fn plan_mirror_capture(
    source_table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
) -> MirrorCaptureResult<MirrorCapturePlan> {
    if primary_key.is_empty() {
        return Err(MirrorCaptureError::MissingPrimaryKey);
    }

    let function_name = QualifiedTableName {
        schema: Some("koldstore".to_string()),
        name: format!("{}_capture", mirror_table.name),
    };
    let source = source_table.quoted();
    let function_sql = capture_function_sql(&function_name, mirror_table, primary_key)?;
    let function = SqlStatement::write("create change-log mirror capture function", &function_sql)
        .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let triggers: [SqlStatement; 3] = MirrorOperation::ALL
        .into_iter()
        .map(|operation| plan_capture_trigger(operation, mirror_table, &source, &function_name))
        .collect::<MirrorCaptureResult<Vec<_>>>()?
        .try_into()
        .expect("mirror capture has exactly three trigger operations");
    let [insert_trigger, update_trigger, delete_trigger] = triggers;
    let drop_triggers = SqlStatement::write(
        "drop change-log mirror capture triggers",
        &MirrorOperation::ALL
            .into_iter()
            .map(|operation| {
                format!(
                    "DROP TRIGGER IF EXISTS {} ON {source}",
                    quote_ident(&operation.capture_trigger_name(&mirror_table.name))
                )
            })
            .collect::<Vec<_>>()
            .join(";\n"),
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let drop_function = SqlStatement::write(
        "drop change-log mirror capture function",
        &format!("DROP FUNCTION IF EXISTS {}()", function_name.quoted()),
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;

    Ok(MirrorCapturePlan {
        function,
        insert_trigger,
        update_trigger,
        delete_trigger,
        drop_triggers,
        drop_function,
    })
}

/// Plans idempotent teardown of mirror capture triggers and function.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_mirror_capture_teardown(
    source_table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
) -> koldstore_common::SqlResult<Vec<SqlStatement>> {
    let function_name = QualifiedTableName {
        schema: Some("koldstore".to_string()),
        name: format!("{}_capture", mirror_table.name),
    };
    let source = source_table.quoted();
    let mut statements = Vec::with_capacity(4);
    for operation in MirrorOperation::ALL {
        let trigger_name = operation.capture_trigger_name(&mirror_table.name);
        statements.push(SqlStatement::write(
            &format!(
                "drop change-log mirror {} capture trigger",
                operation.capture_trigger_suffix()
            ),
            &format!(
                "DROP TRIGGER IF EXISTS {} ON {source}",
                quote_ident(&trigger_name)
            ),
        )?);
    }
    statements.push(SqlStatement::write(
        "drop change-log mirror capture function",
        &format!("DROP FUNCTION IF EXISTS {}()", function_name.quoted()),
    )?);
    Ok(statements)
}

/// Plans dropping a mirror table by relation metadata.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_drop_mirror_table_statement(
    mirror: &MirrorRelation,
) -> koldstore_common::SqlResult<SqlStatement> {
    let drop = koldstore_mirror::plan_drop_mirror_table(mirror);
    SqlStatement::write("demigrate drop change-log mirror", &drop.sql)
}

fn capture_function_sql(
    function_name: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
) -> MirrorCaptureResult<String> {
    let mirror = mirror_table
        .as_table_name()
        .map(MirrorRelation::new)
        .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let pk_names: Vec<&str> = primary_key
        .iter()
        .map(|column| column.column().as_str())
        .collect();
    let pk_values = |row_ref: &str| {
        primary_key
            .iter()
            .map(|column| format!("{row_ref}.{}", quote_ident(column.column().as_str())))
            .collect::<Vec<_>>()
    };
    let insert_upsert = plan_upsert_mirror_row(
        &mirror,
        &pk_names,
        &pk_values("NEW"),
        snowflake_id_call_expression(),
        MirrorOperation::Insert,
        "pg_current_wal_lsn()",
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let update_upsert = plan_upsert_mirror_row(
        &mirror,
        &pk_names,
        &pk_values("NEW"),
        snowflake_id_call_expression(),
        MirrorOperation::Update,
        "pg_current_wal_lsn()",
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let delete_upsert = plan_upsert_mirror_row(
        &mirror,
        &pk_names,
        &pk_values("OLD"),
        snowflake_id_call_expression(),
        MirrorOperation::Delete,
        "pg_current_wal_lsn()",
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let pk_update_guard = primary_key_update_guard(primary_key);

    Ok(format!(
        r#"
CREATE OR REPLACE FUNCTION {function_name}()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, koldstore
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        {insert_upsert}
        PERFORM koldstore.internal_record_row_count_delta(TG_RELID, 1, 1);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        {pk_update_guard}
        {update_upsert}
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        {delete_upsert}
        PERFORM koldstore.internal_record_row_count_delta(TG_RELID, -1, 0);
        RETURN OLD;
    END IF;

    RAISE EXCEPTION 'unsupported pg-koldstore mirror capture operation %', TG_OP;
END;
$$
"#,
        function_name = function_name.quoted()
    ))
}

fn plan_capture_trigger(
    operation: MirrorOperation,
    mirror_table: &QualifiedTableName,
    source_table: &str,
    function_name: &QualifiedTableName,
) -> MirrorCaptureResult<SqlStatement> {
    let trigger_name = operation.capture_trigger_name(&mirror_table.name);
    SqlStatement::write(
        &format!(
            "create change-log mirror {} capture trigger",
            operation.capture_trigger_suffix()
        ),
        &capture_trigger_sql(&trigger_name, operation, source_table, function_name),
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))
}

fn capture_trigger_sql(
    trigger_name: &str,
    operation: MirrorOperation,
    source_table: &str,
    function_name: &QualifiedTableName,
) -> String {
    format!(
        r#"
DROP TRIGGER IF EXISTS {trigger_name} ON {source_table};
CREATE TRIGGER {trigger_name}
AFTER {operation} ON {source_table}
FOR EACH ROW EXECUTE FUNCTION {function_name}()
"#,
        trigger_name = quote_ident(trigger_name),
        operation = operation.sql_trigger_event(),
        function_name = function_name.quoted()
    )
}

fn primary_key_update_guard(primary_key: &[PrimaryKeyColumnShape]) -> String {
    let changed_predicate = primary_key
        .iter()
        .map(|column| {
            let name = quote_ident(column.column().as_str());
            format!("OLD.{name} IS DISTINCT FROM NEW.{name}")
        })
        .collect::<Vec<_>>()
        .join(" OR ");

    format!(
        "IF {changed_predicate} THEN\n            RAISE EXCEPTION 'pg-koldstore does not support primary-key updates on managed table %', TG_TABLE_NAME;\n        END IF;"
    )
}
