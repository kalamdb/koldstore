//! Low-level mirror write SQL builders.

use koldstore_common::{is_safe_identifier, quote_ident, MirrorOperation};

use crate::columns::MirrorColumn;
use crate::{MirrorError, MirrorRelation, MirrorResult, MirrorStatement, SqlParamType};

/// Builds an upsert statement fragment for the latest-state mirror row.
///
/// The value expressions are caller-owned SQL snippets, such as `NEW."id"` in a
/// trigger function or `$1` bind placeholders in direct repository calls.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied or the number of
/// primary-key value expressions differs from the number of columns.
pub fn plan_upsert_mirror_row(
    mirror_table: &MirrorRelation,
    primary_key: &[&str],
    pk_value_expressions: &[String],
    seq_expression: &str,
    operation: MirrorOperation,
    commit_lsn_expression: &str,
) -> MirrorResult<String> {
    if primary_key.is_empty() {
        return Err(MirrorError::MissingPrimaryKey);
    }
    if primary_key.len() != pk_value_expressions.len() {
        return Err(MirrorError::InvalidColumn(
            "primary-key value expression count mismatch".to_string(),
        ));
    }

    let pk_columns = quoted_pk_columns(primary_key)?;
    let mut insert_columns = pk_columns.clone();
    insert_columns.extend(MirrorColumn::insert_quoted_names());

    let mut values = pk_value_expressions.to_vec();
    values.extend([
        seq_expression.to_string(),
        operation.code().to_string(),
        commit_lsn_expression.to_string(),
    ]);

    Ok(format!(
        "INSERT INTO {mirror} ({insert_columns})\n        VALUES ({values})\n        ON CONFLICT ({conflict_columns}) DO UPDATE\n        SET \"seq\" = EXCLUDED.\"seq\",\n            \"op\" = EXCLUDED.\"op\",\n            \"commit_lsn\" = EXCLUDED.\"commit_lsn\";",
        mirror = mirror_table.quoted(),
        insert_columns = insert_columns.join(", "),
        values = values.join(", "),
        conflict_columns = pk_columns.join(", ")
    ))
}

/// Column list for `jsonb_to_recordset` selected-set CTEs.
///
/// Primary-key columns are typed as `text`; mirror metadata includes `seq` and
/// `op` for flush cleanup workflows.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied.
pub fn selected_record_columns(primary_key: &[&str]) -> MirrorResult<String> {
    let pk_columns = quoted_pk_columns(primary_key)?;
    Ok(pk_columns
        .iter()
        .map(|column| format!("{column} text"))
        .chain([
            format!("{} bigint", MirrorColumn::Seq.quoted_name()),
            format!("{} smallint", MirrorColumn::Op.quoted_name()),
        ])
        .collect::<Vec<_>>()
        .join(", "))
}

/// SQL fragment deleting mirror rows joined to a `selected` CTE.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied.
pub fn mirror_delete_using_selected_sql(
    mirror_table: &MirrorRelation,
    primary_key: &[&str],
) -> MirrorResult<String> {
    let join_predicate = mirror_selected_join_predicate(primary_key)?;
    Ok(format!(
        "DELETE FROM {mirror} AS mirror\n    USING selected\n    WHERE {join_predicate}",
        mirror = mirror_table.quoted()
    ))
}

/// Join predicate matching mirror rows to a `selected` CTE by PK and `seq`.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied.
pub fn mirror_selected_join_predicate(primary_key: &[&str]) -> MirrorResult<String> {
    Ok(pk_selected_join_predicates(primary_key)?
        .into_iter()
        .chain(["mirror.\"seq\" = selected.\"seq\"".to_string()])
        .collect::<Vec<_>>()
        .join(" AND "))
}

/// Plans deleting mirror rows from a caller-supplied selected set.
///
/// The selected set must expose primary-key text columns and a `seq` column.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied.
pub fn plan_delete_selected_mirror_rows(
    mirror_table: &MirrorRelation,
    primary_key: &[&str],
    selected_cte_sql: &str,
) -> MirrorResult<MirrorStatement> {
    let delete_sql = mirror_delete_using_selected_sql(mirror_table, primary_key)?;
    Ok(MirrorStatement::write_with_params(
        "delete selected mirror rows",
        format!(
            r#"
WITH selected AS (
{selected_cte_sql}
)
{delete_sql}
"#,
        ),
        [SqlParamType::Jsonb],
    ))
}

/// Validates and quotes primary-key column names.
///
/// # Errors
///
/// Returns an error when the primary key is empty or a column name is unsafe.
pub fn quoted_pk_columns(primary_key: &[&str]) -> MirrorResult<Vec<String>> {
    if primary_key.is_empty() {
        return Err(MirrorError::MissingPrimaryKey);
    }
    primary_key
        .iter()
        .map(|column| {
            let name = column.trim();
            if is_safe_identifier(name) {
                Ok(quote_ident(name))
            } else {
                Err(MirrorError::InvalidColumn(name.to_string()))
            }
        })
        .collect()
}

fn pk_selected_join_predicates(primary_key: &[&str]) -> MirrorResult<Vec<String>> {
    Ok(quoted_pk_columns(primary_key)?
        .into_iter()
        .map(|column| format!("mirror.{column}::text = selected.{column}"))
        .collect())
}
