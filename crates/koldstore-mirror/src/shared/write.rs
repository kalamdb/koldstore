//! Low-level mirror write SQL builders.

use koldstore_common::{is_safe_identifier, quote_ident, MirrorOperation};

use super::columns::MirrorColumn;
use super::error::{MirrorError, MirrorResult};
use super::relation::MirrorRelation;
use super::statement::{MirrorStatement, SqlParamType};

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
    values.extend([seq_expression.to_string(), operation.code().to_string()]);

    Ok(format!(
        "INSERT INTO {mirror} ({insert_columns})\n        VALUES ({values})\n        ON CONFLICT ({conflict_columns}) DO UPDATE\n        SET \"seq\" = EXCLUDED.\"seq\",\n            \"op\" = EXCLUDED.\"op\";",
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

/// Plans a set-based async-mirror upsert from typed `unnest` array binds.
///
/// Bind contract:
/// - `$1` — operation code (`smallint`)
/// - `$2..$N+1` — one `text[]` per primary-key column (cast to `pk_type_names`)
/// - `$N+2` — `bigint[]` of preallocated `seq` values
///
/// Counter result: `(affected_rows, existing_rows)` using `xmax = 0` to detect
/// inserts without an extra PK join.
///
/// # Errors
///
/// Returns an error when the primary key is empty/unsafe or type lists mismatch.
pub fn plan_async_mirror_batch_upsert(
    mirror_quoted: &str,
    primary_key: &[&str],
    pk_type_names: &[String],
) -> MirrorResult<String> {
    if primary_key.len() != pk_type_names.len() {
        return Err(MirrorError::InvalidColumn(
            "primary-key type count mismatch".to_string(),
        ));
    }
    let quoted_keys = quoted_pk_columns(primary_key)?;
    let conflict_keys = quoted_keys.join(", ");
    let pk_count = quoted_keys.len();
    // $1 = op; $2..$(pk_count+1) = pk text arrays; $(pk_count+2) = seq bigint[]
    let mut unnest_args = Vec::with_capacity(pk_count + 1);
    let mut unnest_aliases = Vec::with_capacity(pk_count + 1);
    let mut select_keys = Vec::with_capacity(pk_count);
    for (index, (quoted, type_name)) in quoted_keys.iter().zip(pk_type_names.iter()).enumerate() {
        let param = index + 2;
        let alias = format!("pk_{index}");
        unnest_args.push(format!("${param}::text[]"));
        unnest_aliases.push(alias.clone());
        select_keys.push(format!("incoming.{alias}::{type_name} AS {quoted}"));
    }
    let seq_param = pk_count + 2;
    unnest_args.push(format!("${seq_param}::bigint[]"));
    unnest_aliases.push("seq".to_string());
    select_keys.push("incoming.seq AS \"seq\"".to_string());

    let insert_columns = format!("{conflict_keys}, \"seq\", \"op\"");
    let insert_select = format!(
        "{select_list}, $1::smallint",
        select_list = quoted_keys
            .iter()
            .map(|key| format!("incoming.{key}"))
            .chain(["incoming.\"seq\"".to_string()])
            .collect::<Vec<_>>()
            .join(", ")
    );
    // Rebuild select list with casts into a subquery so INSERT sees typed cols.
    Ok(format!(
        "WITH incoming AS (\
           SELECT {projected} FROM unnest({unnest}) AS incoming({aliases})\
         ), applied AS (\
           INSERT INTO {mirror_quoted} ({insert_columns}) \
           SELECT {insert_select} \
           FROM incoming \
           ON CONFLICT ({conflict_keys}) DO UPDATE \
           SET \"seq\" = EXCLUDED.\"seq\", \
               \"op\" = EXCLUDED.\"op\" \
           RETURNING (xmax = 0) AS inserted\
         ) \
         SELECT count(*)::bigint, \
                count(*) FILTER (WHERE NOT inserted)::bigint \
         FROM applied",
        projected = select_keys.join(", "),
        unnest = unnest_args.join(", "),
        aliases = unnest_aliases.join(", "),
    ))
}

/// Plans a set-based async-mirror insert upsert (alias of unified upsert).
///
/// Retained for callers that still distinguish insert vs update planning; both
/// paths use the same `INSERT … ON CONFLICT` SQL.
///
/// # Errors
///
/// Returns an error when the primary key is empty or unsafe.
pub fn plan_async_mirror_batch_insert(
    mirror_quoted: &str,
    primary_key: &[&str],
    pk_type_names: &[String],
    _seq_expression: &str,
) -> MirrorResult<String> {
    plan_async_mirror_batch_upsert(mirror_quoted, primary_key, pk_type_names)
}

/// Plans a set-based async-mirror update as the unified upsert path.
///
/// UPDATE floods previously used a keyed `UPDATE` that missed pruned rows and
/// paid a large planner/executor cost; upsert matches DELETE tombstone safety.
///
/// # Errors
///
/// Returns an error when the primary key is empty or unsafe.
pub fn plan_async_mirror_batch_update(
    mirror_quoted: &str,
    primary_key: &[&str],
    pk_type_names: &[String],
    _seq_expression: &str,
) -> MirrorResult<String> {
    plan_async_mirror_batch_upsert(mirror_quoted, primary_key, pk_type_names)
}
