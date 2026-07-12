//! Flush catalog SQL plans for cold segments and manifest rows.
//!
//! Manifest assembly and filesystem I/O live in `koldstore-manifest`. This
//! module owns parameterized catalog write plans only. SPI execution stays in
//! `pg_koldstore`. Pending flush reservations live in `pending_catalog`.

use koldstore_common::{ColumnId, SqlStatement};
use koldstore_manifest::SyncState;
use thiserror::Error;

pub use koldstore_manifest::{
    build_manifest_segment_from_catalog_row, load_manifest_from_path, manifest_from_catalog_rows,
    write_manifest_to_path, CatalogManifestSegmentRow, ManifestAssemblyError,
};

/// Flush catalog planning error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SegmentCatalogError {
    /// SQL statement metadata could not be prepared.
    #[error("{0}")]
    Sql(String),
    /// Manifest assembly failed.
    #[error("{0}")]
    Manifest(String),
}

impl From<ManifestAssemblyError> for SegmentCatalogError {
    fn from(error: ManifestAssemblyError) -> Self {
        Self::Manifest(error.to_string())
    }
}

/// Builds indexed column stats JSON for one flushed segment chunk.
#[must_use]
pub fn indexed_column_stats_json(
    column_stats: &std::collections::BTreeMap<ColumnId, (serde_json::Value, serde_json::Value)>,
) -> serde_json::Value {
    let mut values = serde_json::Map::new();
    for (column_id, bounds) in column_stats {
        values.insert(
            column_id.to_string(),
            serde_json::json!({
                "min": bounds.0,
                "max": bounds.1,
            }),
        );
    }
    serde_json::Value::Object(values)
}

/// Plans combined multi-row segment and normalized-stat inserts.
///
/// Segment prune metadata lives in `koldstore.segment_stats` (and the
/// mirrored `column_stats` jsonb). Exact per-PK catalog rows are not written.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_flush_segments_batch_insert() -> Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "flush insert cold segments batch",
        r#"
WITH segment_input AS (
    SELECT *
    FROM unnest(
        $2::uuid[],
        $3::text[],
        $4::integer[],
        $5::bigint[],
        $6::bigint[],
        $7::bigint[],
        $8::bigint[],
        $9::bigint[],
        $10::bigint[],
        $11::integer[],
        $12::jsonb[]
    ) AS u(
        segment_id,
        object_path,
        batch_number,
        min_seq,
        max_seq,
        min_commit_seq,
        max_commit_seq,
        row_count,
        byte_size,
        schema_version,
        column_stats
    )
),
inserted_segments AS (
    INSERT INTO koldstore.segments (
        segment_id,
        table_oid,
        scope_key,
        object_path,
        batch_number,
        min_seq,
        max_seq,
        min_commit_seq,
        max_commit_seq,
        row_count,
        byte_size,
        schema_version,
        column_stats,
        status
    )
    SELECT
        u.segment_id,
        $1::oid,
        '',
        u.object_path,
        u.batch_number,
        u.min_seq,
        u.max_seq,
        u.min_commit_seq,
        u.max_commit_seq,
        u.row_count,
        u.byte_size,
        u.schema_version,
        u.column_stats,
        'staged'
    FROM segment_input u
    RETURNING segment_id, table_oid, scope_key, column_stats
)
INSERT INTO koldstore.segment_stats (
    segment_id,
    table_oid,
    scope_key,
    column_id,
    type_oid,
    min_value,
    max_value
)
SELECT
    cs.segment_id,
    cs.table_oid,
    cs.scope_key,
    stat.column_id::bigint,
    COALESCE(
        (
            SELECT attribute.atttypid
            FROM koldstore.schemas schema_version
            CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(schema_version.columns)
                AS registry_column(value)
            JOIN pg_catalog.pg_attribute attribute
              ON attribute.attrelid = cs.table_oid
             AND attribute.attname = (registry_column.value->>'name')::name
             AND attribute.attnum > 0
             AND NOT attribute.attisdropped
            WHERE schema_version.table_oid = cs.table_oid
              AND schema_version.active
              AND COALESCE((registry_column.value->>'active')::boolean, true)
              AND (registry_column.value->>'column_id')::bigint = stat.column_id::bigint
            ORDER BY schema_version.version DESC
            LIMIT 1
        ),
        'pg_catalog.int8'::regtype::oid
    ),
    pg_catalog.convert_to((stat.bounds->'min')::text, 'UTF8'),
    pg_catalog.convert_to((stat.bounds->'max')::text, 'UTF8')
FROM inserted_segments cs
CROSS JOIN LATERAL pg_catalog.jsonb_each(cs.column_stats)
    AS stat(column_id, bounds)
ON CONFLICT (segment_id, column_id)
DO UPDATE SET
    table_oid = EXCLUDED.table_oid,
    scope_key = EXCLUDED.scope_key,
    type_oid = EXCLUDED.type_oid,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

/// Plans promotion of staged cold segments to query-visible `published`.
///
/// Call after the manifest object and catalog row are durable so merge scan
/// never observes staged files.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_promote_staged_segments_to_published() -> Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "flush promote staged segments published",
        r#"
UPDATE koldstore.segments
SET status = 'published'
WHERE table_oid = $1::oid
  AND segment_id = ANY($2::uuid[])
  AND status = 'staged'
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

/// Plans compaction supersede for replaced published segments.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_supersede_published_segments() -> Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "compaction supersede published segments",
        r#"
UPDATE koldstore.segments
SET status = 'superseded'
WHERE table_oid = $1::oid
  AND segment_id = ANY($2::uuid[])
  AND status = 'published'
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

/// Plans retention begin-delete for superseded segments.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_segments_deleting() -> Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "retention mark segments deleting",
        r#"
UPDATE koldstore.segments
SET status = 'deleting'
WHERE table_oid = $1::oid
  AND segment_id = ANY($2::uuid[])
  AND status IN ('superseded', 'orphaned')
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

/// Plans acknowledge-deleted for segments whose objects were removed.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_segments_deleted() -> Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "gc mark segments deleted",
        r#"
UPDATE koldstore.segments
SET status = 'deleted'
WHERE table_oid = $1::oid
  AND segment_id = ANY($2::uuid[])
  AND status = 'deleting'
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

/// Plans orphan marking for catalog rows without a valid owner/lease.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_mark_segments_orphaned() -> Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "recovery mark segments orphaned",
        r#"
UPDATE koldstore.segments
SET status = 'orphaned'
WHERE table_oid = $1::oid
  AND segment_id = ANY($2::uuid[])
  AND status IN ('staged', 'published')
"#,
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

/// Plans `koldstore.manifest` upsert after flush finalization.
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_manifest_row_upsert() -> Result<SqlStatement, SegmentCatalogError> {
    let in_sync = SyncState::InSync.as_str();
    SqlStatement::write(
        "flush upsert manifest row",
        &format!(
            r#"
INSERT INTO koldstore.manifest (
    table_oid,
    scope_key,
    manifest_path,
    etag,
    generation,
    sync_state,
    segment_count,
    max_seq,
    max_commit_seq,
    last_error,
    updated_at
)
VALUES ($1::oid, '', $2::text, NULL, $3::text, '{in_sync}', $4::integer, $5::bigint, $6::bigint, NULL, now())
ON CONFLICT (table_oid, scope_key)
DO UPDATE SET
    manifest_path = EXCLUDED.manifest_path,
    generation = EXCLUDED.generation,
    sync_state = '{in_sync}',
    segment_count = EXCLUDED.segment_count,
    max_seq = EXCLUDED.max_seq,
    max_commit_seq = EXCLUDED.max_commit_seq,
    last_error = NULL,
    updated_at = now()
"#
        ),
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        plan_flush_segments_batch_insert, plan_mark_segments_deleted, plan_mark_segments_deleting,
        plan_mark_segments_orphaned, plan_promote_staged_segments_to_published,
        plan_supersede_published_segments,
    };

    #[test]
    fn flush_insert_persists_normalized_stats_without_pk_hints() {
        let statement = plan_flush_segments_batch_insert().unwrap();

        assert!(statement.sql.contains("koldstore.segment_stats"));
        assert!(statement.sql.contains("jsonb_each"));
        assert!(!statement.sql.contains("koldstore.cold_pk_hints"));
        assert!(!statement.sql.contains("md5(inserted_segments.object_path)"));
        assert!(statement.sql.contains("'staged'"));
        assert!(!statement.sql.contains("'pending'"));
        assert!(!statement.sql.contains("'published'"));
    }

    #[test]
    fn promote_and_compaction_plans_use_validated_status_edges() {
        let promote = plan_promote_staged_segments_to_published().unwrap();
        assert!(promote.sql.contains("'published'"));
        assert!(promote.sql.contains("status = 'staged'"));

        let supersede = plan_supersede_published_segments().unwrap();
        assert!(supersede.sql.contains("'superseded'"));

        let deleting = plan_mark_segments_deleting().unwrap();
        assert!(deleting.sql.contains("'deleting'"));

        let deleted = plan_mark_segments_deleted().unwrap();
        assert!(deleted.sql.contains("'deleted'"));

        let orphaned = plan_mark_segments_orphaned().unwrap();
        assert!(orphaned.sql.contains("'orphaned'"));
    }
}
