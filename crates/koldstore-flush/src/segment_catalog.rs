//! Flush catalog SQL plans for cold segments and manifest rows.
//!
//! Manifest assembly and filesystem I/O live in `koldstore-manifest`. This
//! module owns parameterized catalog write plans only. SPI execution stays in
//! `pg_koldstore`.
//!
//! Publication protocol: segments insert as `pending` with checksum/etag;
//! [`plan_activate_flush_segments`] CAS-bumps `manifest.generation` and flips
//! those rows to `active` in one statement.

use koldstore_common::SqlStatement;
use koldstore_manifest::SyncState;
use koldstore_parquet::ColdMetadataColumn;
use thiserror::Error;

use crate::stats::FlushStats;

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
    indexed_bounds: &std::collections::BTreeMap<String, (serde_json::Value, serde_json::Value)>,
    stats: &FlushStats,
) -> serde_json::Value {
    let mut values = serde_json::Map::new();
    values.insert(
        ColdMetadataColumn::Seq.name().to_string(),
        serde_json::json!({"min": stats.min_seq, "max": stats.max_seq}),
    );
    for (column, bounds) in indexed_bounds {
        values.insert(
            column.clone(),
            serde_json::json!({
                "min": bounds.0,
                "max": bounds.1,
            }),
        );
    }
    serde_json::Value::Object(values)
}

/// Plans combined multi-row segment and normalized-stat inserts as `pending`.
///
/// Segment prune metadata lives in `koldstore.cold_segment_stats` (and the
/// mirrored `column_stats` jsonb). Exact per-PK catalog rows are not written.
/// Readers ignore `pending` until [`plan_activate_flush_segments`].
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_flush_segments_batch_insert() -> Result<SqlStatement, SegmentCatalogError> {
    SqlStatement::write(
        "flush insert cold segments batch pending",
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
        $12::jsonb[],
        $13::text[],
        $14::text[]
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
        column_stats,
        checksum,
        object_etag
    )
),
inserted_segments AS (
    INSERT INTO koldstore.cold_segments (
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
        status,
        checksum,
        object_etag
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
        'pending',
        u.checksum,
        NULLIF(u.object_etag, '')
    FROM segment_input u
    RETURNING segment_id, table_oid, scope_key, column_stats
)
INSERT INTO koldstore.cold_segment_stats (
    segment_id,
    table_oid,
    scope_key,
    column_name,
    type_oid,
    min_value,
    max_value
)
SELECT
    cs.segment_id,
    cs.table_oid,
    cs.scope_key,
    stat.column_name::name,
    COALESCE(
        (
            SELECT attribute.atttypid
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid = cs.table_oid
              AND attribute.attname = stat.column_name::name
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
        ),
        'pg_catalog.int8'::regtype::oid
    ),
    pg_catalog.convert_to((stat.bounds->'min')::text, 'UTF8'),
    pg_catalog.convert_to((stat.bounds->'max')::text, 'UTF8')
FROM inserted_segments cs
CROSS JOIN LATERAL pg_catalog.jsonb_each(cs.column_stats)
    AS stat(column_name, bounds)
ON CONFLICT (segment_id, column_name)
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

/// Plans CAS generation bump + pending→active activation for one flush.
///
/// Parameters:
/// - `$1` table oid
/// - `$2` expected generation
/// - `$3` new generation (`expected + 1`)
/// - `$4` manifest path
/// - `$5` segment_count
/// - `$6` max_seq
/// - `$7` max_commit_seq
/// - `$8` pending segment id array
///
/// Returns one row with the new generation when CAS succeeds; zero rows on
/// generation conflict (caller must fail the job).
///
/// # Errors
///
/// Returns an error when SQL statement metadata cannot be prepared.
pub fn plan_activate_flush_segments() -> Result<SqlStatement, SegmentCatalogError> {
    let in_sync = SyncState::InSync.as_str();
    SqlStatement::write(
        "flush activate pending segments with generation CAS",
        &format!(
            r#"
WITH cas AS (
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
    VALUES (
        $1::oid,
        '',
        $4::text,
        NULL,
        $3::bigint,
        '{in_sync}',
        $5::integer,
        $6::bigint,
        $7::bigint,
        NULL,
        now()
    )
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
    WHERE koldstore.manifest.generation = $2::bigint
    RETURNING generation
),
activated AS (
    UPDATE koldstore.cold_segments
    SET status = 'active'
    WHERE table_oid = $1::oid
      AND scope_key = ''
      AND status = 'pending'
      AND segment_id = ANY($8::uuid[])
      AND EXISTS (SELECT 1 FROM cas)
    RETURNING segment_id
)
SELECT generation FROM cas
"#
        ),
    )
    .map_err(|error| SegmentCatalogError::Sql(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{plan_activate_flush_segments, plan_flush_segments_batch_insert};

    #[test]
    fn flush_segment_insert_plans_pending_with_checksum() {
        let statement = plan_flush_segments_batch_insert().unwrap();
        assert!(statement.sql.contains("'pending'"));
        assert!(statement.sql.contains("checksum"));
        assert!(statement.sql.contains("object_etag"));
        assert!(!statement.sql.contains("'active'"));
    }

    #[test]
    fn activate_plan_uses_generation_cas() {
        let statement = plan_activate_flush_segments().unwrap();
        assert!(statement
            .sql
            .contains("WHERE koldstore.manifest.generation = $2::bigint"));
        assert!(statement.sql.contains("SET status = 'active'"));
        assert!(statement.sql.contains("segment_id = ANY($8::uuid[])"));
    }
}
