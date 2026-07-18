//! Strict (trigger-based) change-log mirror capture SQL plans.
//!
//!
//! PERFORMANCE: capture uses `FOR EACH STATEMENT` triggers with transition
//! tables so bulk `INSERT`/`UPDATE`/`DELETE` write the mirror in one set-based
//! statement instead of one PL/pgSQL call per row.
//!
//! UPDATE keeps only `NEW TABLE` (half the transition I/O). PK mutation is
//! rejected by a separate column-specific row trigger. INSERT adapts between
//! `ON CONFLICT` (small) and `MERGE` (bulk). UPDATE/DELETE modify existing
//! mirror rows directly under the backfill/activation invariant.

use crate::shared::{quoted_pk_columns, MirrorRelation};
use koldstore_common::{
    quote_ident, session::snowflake_id_call_expression, MirrorOperation, PrimaryKeyColumnShape,
    SqlStatement,
};
use thiserror::Error;

use koldstore_common::QualifiedTableName;

/// Largest INSERT statement kept on PostgreSQL's concurrency-strong
/// `ON CONFLICT` path; larger transition sets use bulk `MERGE`.
const SMALL_INSERT_UPSERT_ROWS: usize = 32;

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
    /// Trigger function that upserts/updates latest-state rows into the mirror.
    pub function: SqlStatement,
    /// Function that rejects an actual primary-key value change.
    pub pk_guard_function: SqlStatement,
    /// INSERT capture trigger.
    pub insert_trigger: SqlStatement,
    /// UPDATE capture trigger.
    pub update_trigger: SqlStatement,
    /// DELETE capture trigger.
    pub delete_trigger: SqlStatement,
    /// Column-specific row trigger; ordinary non-PK updates never invoke it.
    pub pk_guard_trigger: SqlStatement,
    /// Idempotent trigger cleanup statement.
    pub drop_triggers: SqlStatement,
    /// Idempotent capture-function cleanup statement.
    pub drop_function: SqlStatement,
    /// Idempotent PK-guard function cleanup statement.
    pub drop_pk_guard_function: SqlStatement,
}

impl MirrorCapturePlan {
    /// Trigger creation statements in dependency order.
    #[must_use]
    pub fn trigger_statements(&self) -> [&SqlStatement; 4] {
        [
            &self.insert_trigger,
            &self.update_trigger,
            &self.delete_trigger,
            &self.pk_guard_trigger,
        ]
    }

    /// All create statements in dependency order.
    #[must_use]
    pub fn create_statements(&self) -> [&SqlStatement; 6] {
        [
            &self.function,
            &self.pk_guard_function,
            &self.insert_trigger,
            &self.update_trigger,
            &self.delete_trigger,
            &self.pk_guard_trigger,
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
    let guard_function_name = QualifiedTableName {
        schema: Some("koldstore".to_string()),
        name: format!("{}_pk_guard", mirror_table.name),
    };
    let source = source_table.quoted();
    let function_sql = capture_function_sql(&function_name, mirror_table, primary_key)?;
    let function = SqlStatement::write("create change-log mirror capture function", &function_sql)
        .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let pk_guard_function = SqlStatement::write(
        "create change-log mirror primary-key guard function",
        &pk_guard_function_sql(&guard_function_name, primary_key),
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;

    let triggers: [SqlStatement; 3] = MirrorOperation::ALL
        .into_iter()
        .map(|operation| plan_capture_trigger(operation, mirror_table, &source, &function_name))
        .collect::<MirrorCaptureResult<Vec<_>>>()?
        .try_into()
        .expect("mirror capture has exactly three trigger operations");
    let [insert_trigger, update_trigger, delete_trigger] = triggers;
    let pk_guard_trigger =
        plan_pk_guard_trigger(mirror_table, &source, &guard_function_name, primary_key)?;

    let drop_triggers = SqlStatement::write("drop change-log mirror capture triggers", &{
        let mut drops = MirrorOperation::ALL
            .into_iter()
            .map(|operation| {
                format!(
                    "DROP TRIGGER IF EXISTS {} ON {source}",
                    quote_ident(&operation.capture_trigger_name(&mirror_table.name))
                )
            })
            .collect::<Vec<_>>();
        drops.push(format!(
            "DROP TRIGGER IF EXISTS {} ON {source}",
            quote_ident(&pk_guard_trigger_name(&mirror_table.name))
        ));
        for kick_name in async_worker_kick_trigger_names(&mirror_table.name) {
            drops.push(format!(
                "DROP TRIGGER IF EXISTS {} ON {source}",
                quote_ident(&kick_name)
            ));
        }
        // Legacy kick names from earlier async installs.
        for suffix in [
            "_async_worker_kick",
            "_async_worker_kick_ins",
            "_async_worker_kick_upd",
            "_async_worker_kick_del",
        ] {
            drops.push(format!(
                "DROP TRIGGER IF EXISTS {} ON {source}",
                quote_ident(&truncate_pg_identifier(&format!(
                    "{mirror_table_name}{suffix}",
                    mirror_table_name = &mirror_table.name
                )))
            ));
        }
        drops.join(";\n")
    })
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let drop_function = SqlStatement::write(
        "drop change-log mirror capture function",
        &format!("DROP FUNCTION IF EXISTS {}()", function_name.quoted()),
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let drop_pk_guard_function = SqlStatement::write(
        "drop change-log mirror primary-key guard function",
        &format!("DROP FUNCTION IF EXISTS {}()", guard_function_name.quoted()),
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;

    Ok(MirrorCapturePlan {
        function,
        pk_guard_function,
        insert_trigger,
        update_trigger,
        delete_trigger,
        pk_guard_trigger,
        drop_triggers,
        drop_function,
        drop_pk_guard_function,
    })
}

/// Plans idempotent teardown of mirror capture triggers and functions.
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
    let guard_function_name = QualifiedTableName {
        schema: Some("koldstore".to_string()),
        name: format!("{}_pk_guard", mirror_table.name),
    };
    let source = source_table.quoted();
    let mut statements = Vec::with_capacity(7);
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
        "drop change-log mirror primary-key guard trigger",
        &format!(
            "DROP TRIGGER IF EXISTS {} ON {source}",
            quote_ident(&pk_guard_trigger_name(&mirror_table.name))
        ),
    )?);
    statements.push(SqlStatement::write(
        "drop async mirror worker kick triggers",
        &{
            let mut drops = async_worker_kick_trigger_names(&mirror_table.name)
                .into_iter()
                .map(|kick_name| {
                    format!(
                        "DROP TRIGGER IF EXISTS {} ON {source}",
                        quote_ident(&kick_name)
                    )
                })
                .collect::<Vec<_>>();
            for suffix in [
                "_async_worker_kick",
                "_async_worker_kick_ins",
                "_async_worker_kick_upd",
                "_async_worker_kick_del",
            ] {
                drops.push(format!(
                    "DROP TRIGGER IF EXISTS {} ON {source}",
                    quote_ident(&truncate_pg_identifier(&format!(
                        "{}{suffix}",
                        &mirror_table.name
                    )))
                ));
            }
            drops.join(";\n")
        },
    )?);
    statements.push(SqlStatement::write(
        "drop change-log mirror capture function",
        &format!("DROP FUNCTION IF EXISTS {}()", function_name.quoted()),
    )?);
    statements.push(SqlStatement::write(
        "drop change-log mirror primary-key guard function",
        &format!("DROP FUNCTION IF EXISTS {}()", guard_function_name.quoted()),
    )?);
    Ok(statements)
}

/// Returns the deterministic statement triggers that keep the async applier running.
///
/// PostgreSQL forbids transition tables on multi-event triggers, so INSERT /
/// UPDATE / DELETE each get their own kick trigger. Suffixes are short and
/// reserved before truncating the mirror-table prefix so long names stay unique.
#[must_use]
pub fn async_worker_kick_trigger_names(mirror_table_name: &str) -> [String; 3] {
    [
        trigger_name_with_suffix(mirror_table_name, "_aki"),
        trigger_name_with_suffix(mirror_table_name, "_aku"),
        trigger_name_with_suffix(mirror_table_name, "_akd"),
    ]
}

/// Legacy single kick trigger name kept for teardown of older installs.
#[must_use]
pub fn async_worker_kick_trigger_name(mirror_table_name: &str) -> String {
    truncate_pg_identifier(&format!("{mirror_table_name}_async_worker_kick"))
}

fn trigger_name_with_suffix(mirror_table_name: &str, suffix: &str) -> String {
    let max_base = 63usize.saturating_sub(suffix.len());
    let mut end = mirror_table_name.len().min(max_base);
    while end > 0 && !mirror_table_name.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{suffix}", &mirror_table_name[..end])
}

fn truncate_pg_identifier(identifier: &str) -> String {
    const MAX_IDENTIFIER_BYTES: usize = 63;
    if identifier.len() <= MAX_IDENTIFIER_BYTES {
        return identifier.to_string();
    }
    let mut end = MAX_IDENTIFIER_BYTES;
    while !identifier.is_char_boundary(end) {
        end -= 1;
    }
    identifier[..end].to_string()
}

/// Plans removal of only the statement-level mirror DML triggers.
///
/// Async WAL capture uses this after strict migration/backfill activation. The
/// primary-key mutation guard remains installed because logical decoding does
/// not change the managed-table PK contract.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_drop_mirror_dml_triggers(
    source_table: &QualifiedTableName,
    mirror_table: &QualifiedTableName,
) -> koldstore_common::SqlResult<SqlStatement> {
    let source = source_table.quoted();
    let sql = MirrorOperation::ALL
        .into_iter()
        .map(|operation| {
            format!(
                "DROP TRIGGER IF EXISTS {} ON {source}",
                quote_ident(&operation.capture_trigger_name(&mirror_table.name))
            )
        })
        .collect::<Vec<_>>()
        .join(";\n");
    SqlStatement::write("switch mirror capture to async WAL", &sql)
}

/// Plans dropping a mirror table by relation metadata.
///
/// # Errors
///
/// Returns an error when statement metadata cannot be prepared.
pub fn plan_drop_mirror_table_statement(
    mirror: &MirrorRelation,
) -> koldstore_common::SqlResult<SqlStatement> {
    let drop = crate::shared::plan_drop_mirror_table(mirror);
    SqlStatement::write("demigrate drop change-log mirror", &drop.sql)
}

fn pk_guard_trigger_name(mirror_table_name: &str) -> String {
    format!("{mirror_table_name}_pk_update_guard")
}

fn pk_equality_predicate(primary_key: &[PrimaryKeyColumnShape], left: &str, right: &str) -> String {
    primary_key
        .iter()
        .map(|column| {
            let name = quote_ident(column.column().as_str());
            format!("{left}.{name} = {right}.{name}")
        })
        .collect::<Vec<_>>()
        .join(" AND ")
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
    let pk_columns =
        quoted_pk_columns(&pk_names).map_err(|error| MirrorCaptureError::Sql(error.to_string()))?;
    let insert_columns = {
        let mut columns = pk_columns.clone();
        columns.extend([
            "\"seq\"".to_string(),
            "\"op\"".to_string(),
            "\"commit_lsn\"".to_string(),
        ]);
        columns.join(", ")
    };
    let pk_select_src = pk_columns
        .iter()
        .map(|column| format!("src.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let pk_select_incoming = pk_columns
        .iter()
        .map(|column| format!("incoming.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let seq_expr = snowflake_id_call_expression();
    let conflict_columns = pk_columns.join(", ");
    let pk_join = pk_equality_predicate(primary_key, "mirror", "src");
    let merge_on = pk_equality_predicate(primary_key, "mirror", "incoming");
    let insert_op = MirrorOperation::Insert.code();
    let update_op = MirrorOperation::Update.code();
    let delete_op = MirrorOperation::Delete.code();

    // Small statements retain ON CONFLICT's insert-or-update concurrency guarantee.
    // Bulk statements use MERGE because PostgreSQL's conflict-resolution machinery
    // dominates capture time when most incoming keys are new.
    let small_insert = format!(
        r#"
        INSERT INTO {mirror} ({insert_columns})
        SELECT {pk_select_src}, {seq_expr}, {insert_op}, capture_wal_lsn
        FROM new_rows AS src
        ON CONFLICT ({conflict_columns}) DO UPDATE
        SET "seq" = EXCLUDED."seq",
            "op" = EXCLUDED."op",
            "commit_lsn" = EXCLUDED."commit_lsn""#,
        mirror = mirror.quoted(),
    );

    let bulk_insert = format!(
        r#"
        MERGE INTO {mirror} AS mirror
        USING (
            SELECT {pk_select_src},
                   {seq_expr} AS next_seq
            FROM new_rows AS src
        ) AS incoming
        ON {merge_on}
        WHEN MATCHED THEN
            UPDATE SET "seq" = incoming.next_seq,
                       "op" = {insert_op},
                       "commit_lsn" = capture_wal_lsn
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({pk_select_incoming}, incoming.next_seq, {insert_op}, capture_wal_lsn)"#,
        mirror = mirror.quoted(),
    );

    // Every mutable hot row has a mirror row: empty-table INSERT capture and
    // existing-table backfill establish it before activation, and flush removes the
    // corresponding hot row when it removes a live mirror entry. That invariant
    // lets UPDATE and DELETE modify the mirror directly instead of upserting.
    let direct_update = format!(
        r#"
        UPDATE {mirror} AS mirror
        SET "seq" = {seq_expr},
            "op" = {update_op},
            "commit_lsn" = capture_wal_lsn
        FROM new_rows AS src
        WHERE {pk_join};
        GET DIAGNOSTICS affected = ROW_COUNT;
        IF affected = 0 THEN
            PERFORM 1 FROM new_rows LIMIT 1;
            IF FOUND THEN
                RAISE EXCEPTION
                    'pg-koldstore mirror invariant violated for managed table %',
                    TG_TABLE_NAME;
            END IF;
        END IF;"#,
        mirror = mirror.quoted(),
    );

    let direct_delete = format!(
        r#"
        UPDATE {mirror} AS mirror
        SET "seq" = {seq_expr},
            "op" = {delete_op},
            "commit_lsn" = capture_wal_lsn
        FROM old_rows AS src
        WHERE {pk_join};
        GET DIAGNOSTICS affected = ROW_COUNT;
        IF affected = 0 THEN
            PERFORM 1 FROM old_rows LIMIT 1;
            IF FOUND THEN
                RAISE EXCEPTION
                    'pg-koldstore mirror invariant violated for managed table %',
                    TG_TABLE_NAME;
            END IF;
        END IF;
        PERFORM koldstore.internal_record_row_count_delta(TG_RELID, -affected, 0);"#,
        mirror = mirror.quoted(),
    );

    Ok(format!(
        r#"
CREATE OR REPLACE FUNCTION {function_name}()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, koldstore
AS $$
DECLARE
    affected bigint;
    existing_mirror_rows bigint := 0;
    capture_wal_lsn pg_lsn := pg_current_wal_lsn();
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Pre-count overlapping PKs so reinserts over tombstones do not inflate
        -- mirror_row_count; MERGE has no RETURNING on supported PostgreSQL majors.
        SELECT count(*)
        INTO existing_mirror_rows
        FROM new_rows AS src
        JOIN {mirror} AS mirror
          ON {pk_join};
        IF EXISTS (
            SELECT 1 FROM new_rows OFFSET {small_threshold} LIMIT 1
        ) THEN
            {bulk_insert};
        ELSE
            {small_insert};
        END IF;
        GET DIAGNOSTICS affected = ROW_COUNT;
        PERFORM koldstore.internal_record_row_count_delta(
            TG_RELID,
            affected,
            affected - existing_mirror_rows
        );
        RETURN NULL;
    ELSIF TG_OP = 'UPDATE' THEN
        {direct_update}
        RETURN NULL;
    ELSIF TG_OP = 'DELETE' THEN
        {direct_delete}
        RETURN NULL;
    END IF;

    RAISE EXCEPTION 'unsupported pg-koldstore mirror capture operation %', TG_OP;
END;
$$
"#,
        function_name = function_name.quoted(),
        mirror = mirror.quoted(),
        small_threshold = SMALL_INSERT_UPSERT_ROWS,
    ))
}

fn pk_guard_function_sql(
    function_name: &QualifiedTableName,
    primary_key: &[PrimaryKeyColumnShape],
) -> String {
    let distinct = primary_key
        .iter()
        .map(|column| {
            let name = quote_ident(column.column().as_str());
            format!("OLD.{name} IS DISTINCT FROM NEW.{name}")
        })
        .collect::<Vec<_>>()
        .join("\n       OR ");

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
            'pg-koldstore does not support primary-key updates on managed table %',
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
) -> MirrorCaptureResult<SqlStatement> {
    let trigger_name = pk_guard_trigger_name(&mirror_table.name);
    let of_columns = primary_key
        .iter()
        .map(|column| quote_ident(column.column().as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    SqlStatement::write(
        "create change-log mirror primary-key guard trigger",
        &format!(
            r#"
DROP TRIGGER IF EXISTS {trigger_name} ON {source_table};
CREATE TRIGGER {trigger_name}
BEFORE UPDATE OF {of_columns} ON {source_table}
FOR EACH ROW EXECUTE FUNCTION {function_name}()
"#,
            trigger_name = quote_ident(&trigger_name),
            function_name = function_name.quoted(),
        ),
    )
    .map_err(|error| MirrorCaptureError::Sql(error.to_string()))
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
    // PK mutation is rejected by a separate column-specific row trigger. Keeping
    // OLD TABLE here would materialize a second full-width transition relation for
    // every ordinary UPDATE and would reintroduce the quadratic old/new PK check.
    let referencing = match operation {
        MirrorOperation::Insert => "REFERENCING NEW TABLE AS new_rows",
        MirrorOperation::Update => "REFERENCING NEW TABLE AS new_rows",
        MirrorOperation::Delete => "REFERENCING OLD TABLE AS old_rows",
    };
    format!(
        r#"
DROP TRIGGER IF EXISTS {trigger_name} ON {source_table};
CREATE TRIGGER {trigger_name}
AFTER {operation} ON {source_table}
{referencing}
FOR EACH STATEMENT EXECUTE FUNCTION {function_name}()
"#,
        trigger_name = quote_ident(trigger_name),
        operation = operation.sql_trigger_event(),
        function_name = function_name.quoted()
    )
}
