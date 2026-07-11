//! Flush catalog SQL plans for `koldstore.pending` reservations.
//!
//! Pending rows are approximate `(table_oid, scope_key) → row_count` counters,
//! not cold files. Cold objects live only in `koldstore.segments`.

use koldstore_common::SqlStatement;
use thiserror::Error;

use crate::pre_flush::PendingSegmentPlan;

/// Pending-catalog planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PendingCatalogError {
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
}

/// One pending reservation ready to upsert into `koldstore.pending`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingUpsert {
    /// Catalog `scope_key` (empty for shared).
    pub scope_key: String,
    /// Estimated rows reserved from the in-memory counter.
    pub row_count: i64,
    /// Active schema version at reservation time.
    pub schema_version: i32,
}

/// Materializes pending upserts from pre-flush plans.
#[must_use]
pub fn materialize_pending_upserts(
    schema_version: i32,
    plans: &[PendingSegmentPlan],
) -> Vec<PendingUpsert> {
    plans
        .iter()
        .map(|plan| PendingUpsert {
            scope_key: plan.key.catalog_scope_key().to_string(),
            row_count: i64::try_from(plan.row_count).unwrap_or(i64::MAX),
            schema_version,
        })
        .collect()
}

/// Plans create-or-update of pending reservations from approximate counters.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_upsert_pending() -> Result<SqlStatement, PendingCatalogError> {
    SqlStatement::write(
        "flush upsert pending",
        r#"
WITH input AS (
    SELECT *
    FROM unnest(
        $2::text[],
        $3::bigint[],
        $4::integer[]
    ) AS u(scope_key, row_count, schema_version)
)
INSERT INTO koldstore.pending (
    table_oid,
    scope_key,
    row_count,
    schema_version,
    updated_at
)
SELECT
    $1::oid,
    i.scope_key,
    i.row_count,
    i.schema_version,
    now()
FROM input AS i
ON CONFLICT (table_oid, scope_key)
DO UPDATE SET
    row_count = EXCLUDED.row_count,
    schema_version = EXCLUDED.schema_version,
    updated_at = now()
"#,
    )
    .map_err(|error| PendingCatalogError::Sql(error.to_string()))
}

/// Plans a lookup of pending reservations for one table.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_list_pending() -> Result<SqlStatement, PendingCatalogError> {
    SqlStatement::read_with_params(
        "list pending",
        r#"
SELECT COALESCE(jsonb_agg(
    jsonb_build_object(
        'scope_key', scope_key,
        'row_count', row_count,
        'schema_version', schema_version
    )
    ORDER BY scope_key
)::text, '[]')
FROM koldstore.pending
WHERE table_oid = $1::oid
"#,
        [koldstore_common::SqlParamType::Oid],
    )
    .map_err(|error| PendingCatalogError::Sql(error.to_string()))
}

/// Plans deletion of pending reservations for selected scopes.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_delete_pending_for_scopes() -> Result<SqlStatement, PendingCatalogError> {
    SqlStatement::write(
        "delete pending for scopes",
        r#"
DELETE FROM koldstore.pending
WHERE table_oid = $1::oid
  AND scope_key = ANY ($2::text[])
"#,
    )
    .map_err(|error| PendingCatalogError::Sql(error.to_string()))
}

/// Plans deletion of all pending reservations for a table.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_delete_pending() -> Result<SqlStatement, PendingCatalogError> {
    SqlStatement::write_with_params(
        "delete pending",
        r#"
DELETE FROM koldstore.pending
WHERE table_oid = $1::oid
"#,
        [koldstore_common::SqlParamType::Oid],
    )
    .map_err(|error| PendingCatalogError::Sql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        plan_delete_pending, plan_delete_pending_for_scopes, plan_list_pending, plan_upsert_pending,
    };

    #[test]
    fn pending_plans_target_pending_table_not_segments() {
        let upsert = plan_upsert_pending().unwrap();
        assert!(upsert.sql.contains("koldstore.pending"));
        assert!(upsert.sql.contains("ON CONFLICT (table_oid, scope_key)"));
        assert!(!upsert.sql.contains("koldstore.segments"));
        assert!(!upsert.sql.contains("object_path"));
        assert!(!upsert.sql.contains("status"));

        let list = plan_list_pending().unwrap();
        assert!(list.sql.contains("FROM koldstore.pending"));
        assert!(!list.sql.contains("koldstore.segments"));

        let delete_scopes = plan_delete_pending_for_scopes().unwrap();
        assert!(delete_scopes.sql.contains("DELETE FROM koldstore.pending"));
        assert!(!delete_scopes.sql.contains("koldstore.segments"));

        let delete_all = plan_delete_pending().unwrap();
        assert!(delete_all.sql.contains("DELETE FROM koldstore.pending"));
        assert!(!delete_all.sql.contains("koldstore.segments"));
    }
}
