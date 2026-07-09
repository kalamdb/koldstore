//! Hot/cold materialize SQL template for managed reads.

/// Hot heap rows use a sentinel sequence during SQL merge resolution.
pub const HOT_SEQ_SENTINEL: i64 = i64::MAX;

/// Inputs for the managed hot/cold materialize query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializeQueryParts {
    /// Fully quoted hot table reference.
    pub table_sql: String,
    /// `jsonb_build_object(...)` expression for hot primary keys.
    pub hot_pk: String,
    /// Winner projection SQL.
    pub projection: String,
    /// SQL-literal escaped cold row JSON payload.
    pub cold_rows_json: String,
    /// Residual predicate suffix such as ` AND (...)` or empty.
    pub residual_where: String,
    /// Hot-row sequence expression.
    pub hot_seq: i64,
}

/// Builds the hot/cold winner materialize SQL used by the CustomScan executor.
#[must_use]
pub fn build_materialize_query(parts: &MaterializeQueryParts) -> String {
    format!(
        r#"
WITH hot AS (
    SELECT
        to_jsonb(hot) AS row_image,
        jsonb_build_object({hot_pk}) AS pk_json,
        {hot_seq}::bigint AS seq,
        {hot_seq}::bigint AS commit_seq,
        false AS deleted,
        true AS from_hot
    FROM ONLY {table} AS hot
),
candidates AS (
    SELECT row_image, pk_json, seq, commit_seq, deleted, false AS from_hot
    FROM jsonb_to_recordset('{cold_rows_json}'::jsonb) AS cold(
        pk_json jsonb,
        row_image jsonb,
        seq bigint,
        commit_seq bigint,
        deleted boolean,
        schema_version integer
    )
    UNION ALL
    SELECT row_image, pk_json, seq, commit_seq, deleted, from_hot
    FROM hot
),
winners AS (
    SELECT DISTINCT ON (pk_json::text)
        row_image,
        deleted
    FROM candidates
    ORDER BY pk_json::text, seq DESC, commit_seq DESC, from_hot DESC
)
SELECT {projection}
FROM winners
WHERE NOT deleted{residual_where}
"#,
        hot_pk = parts.hot_pk,
        hot_seq = parts.hot_seq,
        table = parts.table_sql,
        cold_rows_json = parts.cold_rows_json,
        projection = parts.projection,
        residual_where = parts.residual_where,
    )
}
