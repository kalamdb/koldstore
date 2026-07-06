//! Low-level mirror read SQL builders.

use crate::write::quoted_pk_columns;
use crate::{MirrorRelation, MirrorResult, MirrorStatement, SqlParamType};

/// Plans the mirror half of `koldstore.changes_since`.
///
/// Additional predicates are caller-owned validated SQL fragments. This lets
/// pg_koldstore keep scope policy and API semantics outside the storage crate.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied.
pub fn plan_select_mirror_rows_after_seq(
    mirror_table: &MirrorRelation,
    primary_key: &[&str],
    since_param: usize,
    limit_param: usize,
    additional_predicates: &[String],
) -> MirrorResult<MirrorStatement> {
    plan_select_mirror_rows_after_seq_with_params(
        mirror_table,
        primary_key,
        since_param,
        limit_param,
        additional_predicates,
        &[],
    )
}

/// Plans the mirror half of `koldstore.changes_since` with explicit caller
/// parameter metadata for additional predicates.
///
/// # Errors
///
/// Returns an error when no primary-key columns are supplied.
pub fn plan_select_mirror_rows_after_seq_with_params(
    mirror_table: &MirrorRelation,
    primary_key: &[&str],
    since_param: usize,
    limit_param: usize,
    additional_predicates: &[String],
    additional_param_types: &[(usize, SqlParamType)],
) -> MirrorResult<MirrorStatement> {
    let pk_json = pk_json_projection(primary_key)?;
    let mut where_clauses = vec![format!("mirror.\"seq\" > ${since_param}::bigint")];
    where_clauses.extend(additional_predicates.iter().cloned());
    let mut param_types = param_types_for_slots(
        since_param,
        SqlParamType::BigInt,
        limit_param,
        SqlParamType::Integer,
        additional_param_types,
    );
    Ok(MirrorStatement::read_with_params(
        "changes_since from change-log mirror",
        format!(
            r#"SELECT
    mirror."seq" AS commit_seq,
    mirror."seq" AS seq,
    mirror."op" AS op,
    jsonb_build_object({pk_json}) AS pk,
    (mirror."op" = 3) AS deleted,
    NULL::jsonb AS row_image
FROM {mirror} AS mirror
WHERE {where_clause}
ORDER BY mirror."seq" ASC
LIMIT ${limit_param}::integer"#,
            mirror = mirror_table.quoted(),
            where_clause = where_clauses.join(" AND ")
        ),
        std::mem::take(&mut param_types),
    ))
}

/// Plans aggregate stats over one mirror table.
#[must_use]
pub fn plan_mirror_stats(mirror_table: &MirrorRelation) -> MirrorStatement {
    MirrorStatement::read(
        "select mirror stats",
        format!(
            r#"SELECT jsonb_build_object(
    'row_count', count(*),
    'min_seq', COALESCE(min("seq"), 0),
    'max_seq', COALESCE(max("seq"), 0),
    'min_commit_seq', COALESCE(min("seq"), 0),
    'max_commit_seq', COALESCE(max("seq"), 0)
)::text
FROM {mirror}"#,
            mirror = mirror_table.quoted()
        ),
    )
}

fn pk_json_projection(primary_key: &[&str]) -> MirrorResult<String> {
    let quoted = quoted_pk_columns(primary_key)?;
    Ok(primary_key
        .iter()
        .zip(quoted)
        .map(|(column, quoted)| format!("'{}', mirror.{quoted}", column.trim()))
        .collect::<Vec<_>>()
        .join(", "))
}

fn param_types_for_slots(
    first_slot: usize,
    first_type: SqlParamType,
    second_slot: usize,
    second_type: SqlParamType,
    additional: &[(usize, SqlParamType)],
) -> Vec<SqlParamType> {
    let max_slot = additional
        .iter()
        .map(|(slot, _)| *slot)
        .chain([first_slot, second_slot])
        .max()
        .unwrap_or(0);
    let mut param_types = vec![SqlParamType::Text; max_slot];
    if first_slot > 0 {
        param_types[first_slot - 1] = first_type;
    }
    if second_slot > 0 {
        param_types[second_slot - 1] = second_type;
    }
    for (slot, param_type) in additional {
        if *slot > 0 {
            param_types[*slot - 1] = *param_type;
        }
    }
    param_types
}
