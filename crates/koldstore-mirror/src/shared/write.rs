//! Low-level mirror write SQL builders.

use koldstore_common::{is_safe_identifier, quote_ident, MirrorOperation};

use super::columns::MirrorColumn;
use super::error::{MirrorError, MirrorResult};
use super::relation::MirrorRelation;

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

/// Plans a set-based async-mirror upsert from typed `unnest` array binds.
///
/// Bind contract:
/// - `$1` — operation code (`smallint`)
/// - `$2..$N+1` — one `text[]` per primary-key column (cast to `pk_type_names`)
/// - `$N+2` — `bigint[]` of preallocated `seq` values
/// - `$N+3` — `bytea[]` of Sort Key V1 `order_key` values when `include_order_key`
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
    include_order_key: bool,
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
    // optional $(pk_count+3) = order_key bytea[]
    let mut unnest_args = Vec::with_capacity(pk_count + 2);
    let mut unnest_aliases = Vec::with_capacity(pk_count + 2);
    let mut select_keys = Vec::with_capacity(pk_count + 2);
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
    if include_order_key {
        let order_param = pk_count + 3;
        unnest_args.push(format!("${order_param}::bytea[]"));
        unnest_aliases.push("order_key".to_string());
        select_keys.push("incoming.order_key AS \"order_key\"".to_string());
    }

    let insert_columns = if include_order_key {
        format!("{conflict_keys}, \"order_key\", \"seq\", \"op\"")
    } else {
        format!("{conflict_keys}, \"seq\", \"op\"")
    };
    let insert_select_cols = quoted_keys
        .iter()
        .map(|key| format!("incoming.{key}"))
        .chain(include_order_key.then(|| "incoming.\"order_key\"".to_string()))
        .chain(["incoming.\"seq\"".to_string(), "$1::smallint".to_string()])
        .collect::<Vec<_>>()
        .join(", ");
    // Order keys are immutable per PK; keep the first encoded value on conflict.
    let conflict_set = "\"seq\" = EXCLUDED.\"seq\", \
               \"op\" = EXCLUDED.\"op\"";
    Ok(format!(
        "WITH incoming AS (\
           SELECT {projected} FROM unnest({unnest}) AS incoming({aliases})\
         ), applied AS (\
           INSERT INTO {mirror_quoted} ({insert_columns}) \
           SELECT {insert_select_cols} \
           FROM incoming \
           ON CONFLICT ({conflict_keys}) DO UPDATE \
           SET {conflict_set} \
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

/// Plans a set-based async-mirror update with an insert-missing fallback.
///
/// Existing mirror rows take the direct `UPDATE` path. Rows absent after flush
/// pruning are restored by the following conflict-safe insert, preserving
/// latest-state replay semantics without paying conflict resolution for the
/// common hot-row case.
///
/// When `include_order_key` is true, bind `$N+3` as `bytea[]` and persist
/// `order_key` on both the update and insert-missing paths.
///
/// # Errors
///
/// Returns an error when the primary key is empty or unsafe.
pub fn plan_async_mirror_batch_update(
    mirror_quoted: &str,
    primary_key: &[&str],
    pk_type_names: &[String],
    include_order_key: bool,
) -> MirrorResult<String> {
    if primary_key.len() != pk_type_names.len() {
        return Err(MirrorError::InvalidColumn(
            "primary-key type count mismatch".to_string(),
        ));
    }
    let quoted_keys = quoted_pk_columns(primary_key)?;
    let conflict_keys = quoted_keys.join(", ");
    let pk_count = quoted_keys.len();
    let mut unnest_args = Vec::with_capacity(pk_count + 2);
    let mut unnest_aliases = Vec::with_capacity(pk_count + 2);
    let mut projected = Vec::with_capacity(pk_count + 2);
    for (index, (quoted, type_name)) in quoted_keys.iter().zip(pk_type_names.iter()).enumerate() {
        let param = index + 2;
        let alias = format!("pk_{index}");
        unnest_args.push(format!("${param}::text[]"));
        unnest_aliases.push(alias.clone());
        projected.push(format!("incoming.{alias}::{type_name} AS {quoted}"));
    }
    let seq_param = pk_count + 2;
    unnest_args.push(format!("${seq_param}::bigint[]"));
    unnest_aliases.push("seq".to_string());
    projected.push("incoming.seq AS \"seq\"".to_string());
    if include_order_key {
        let order_param = pk_count + 3;
        unnest_args.push(format!("${order_param}::bytea[]"));
        unnest_aliases.push("order_key".to_string());
        projected.push("incoming.order_key AS \"order_key\"".to_string());
    }

    let update_join = quoted_keys
        .iter()
        .map(|key| format!("mirror.{key} = incoming.{key}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let missing_join = quoted_keys
        .iter()
        .map(|key| format!("updated.{key} = incoming.{key}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let returning_keys = quoted_keys
        .iter()
        .map(|key| format!("mirror.{key}"))
        .collect::<Vec<_>>()
        .join(", ");
    let update_set = "\"seq\" = incoming.\"seq\", \
               \"op\" = $1::smallint";
    let incoming_values = quoted_keys
        .iter()
        .map(|key| format!("incoming.{key}"))
        .chain(include_order_key.then(|| "incoming.\"order_key\"".to_string()))
        .chain(["incoming.\"seq\"".to_string(), "$1::smallint".to_string()])
        .collect::<Vec<_>>()
        .join(", ");
    let insert_columns = if include_order_key {
        format!("{conflict_keys}, \"order_key\", \"seq\", \"op\"")
    } else {
        format!("{conflict_keys}, \"seq\", \"op\"")
    };
    // Order keys are immutable per PK; keep the first encoded value on conflict.
    let conflict_set = "\"seq\" = EXCLUDED.\"seq\", \
               \"op\" = EXCLUDED.\"op\"";
    let missing_key = quoted_keys
        .first()
        .expect("primary-key planner rejects an empty key");

    Ok(format!(
        "WITH incoming AS (\
           SELECT {projected} FROM unnest({unnest}) AS incoming({aliases})\
         ), updated AS (\
           UPDATE {mirror_quoted} AS mirror \
           SET {update_set} \
           FROM incoming \
           WHERE {update_join} \
           RETURNING {returning_keys}\
         ), inserted AS (\
           INSERT INTO {mirror_quoted} ({insert_columns}) \
           SELECT {incoming_values} \
           FROM incoming \
           LEFT JOIN updated ON {missing_join} \
           WHERE updated.{missing_key} IS NULL \
           ON CONFLICT ({conflict_keys}) DO UPDATE \
           SET {conflict_set} \
           RETURNING (xmax = 0) AS inserted\
         ) \
         SELECT ((SELECT count(*) FROM updated) + count(*))::bigint, \
                ((SELECT count(*) FROM updated) + \
                 count(*) FILTER (WHERE NOT inserted))::bigint \
         FROM inserted",
        projected = projected.join(", "),
        unnest = unnest_args.join(", "),
        aliases = unnest_aliases.join(", "),
    ))
}

/// Plans a delete-op update that only touches existing mirror rows.
///
/// Used when `order_key` is required: DELETE WAL often lacks the order column
/// under default replica identity, so inventing a tombstone insert is unsafe.
///
/// Bind contract matches upsert without `order_key`: `$1` op, `$2..$N+1` PK
/// arrays, `$N+2` seq array.
///
/// # Errors
///
/// Returns an error when the primary key is empty/unsafe or type lists mismatch.
pub fn plan_async_mirror_batch_delete_existing(
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
    let pk_count = quoted_keys.len();
    let mut unnest_args = Vec::with_capacity(pk_count + 1);
    let mut unnest_aliases = Vec::with_capacity(pk_count + 1);
    let mut projected = Vec::with_capacity(pk_count + 1);
    for (index, (quoted, type_name)) in quoted_keys.iter().zip(pk_type_names.iter()).enumerate() {
        let param = index + 2;
        let alias = format!("pk_{index}");
        unnest_args.push(format!("${param}::text[]"));
        unnest_aliases.push(alias.clone());
        projected.push(format!("incoming.{alias}::{type_name} AS {quoted}"));
    }
    let seq_param = pk_count + 2;
    unnest_args.push(format!("${seq_param}::bigint[]"));
    unnest_aliases.push("seq".to_string());
    projected.push("incoming.seq AS \"seq\"".to_string());
    let update_join = quoted_keys
        .iter()
        .map(|key| format!("mirror.{key} = incoming.{key}"))
        .collect::<Vec<_>>()
        .join(" AND ");

    Ok(format!(
        "WITH incoming AS (\
           SELECT {projected} FROM unnest({unnest}) AS incoming({aliases})\
         ), updated AS (\
           UPDATE {mirror_quoted} AS mirror \
           SET \"seq\" = incoming.\"seq\", \
               \"op\" = $1::smallint \
           FROM incoming \
           WHERE {update_join} \
           RETURNING 1\
         ) \
         SELECT count(*)::bigint, count(*)::bigint FROM updated",
        projected = projected.join(", "),
        unnest = unnest_args.join(", "),
        aliases = unnest_aliases.join(", "),
    ))
}
