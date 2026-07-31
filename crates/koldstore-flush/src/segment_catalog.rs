//! Flush catalog SQL plans for cold segments and manifest rows.
//!
//! Manifest assembly and filesystem I/O live in `koldstore-manifest`. This
//! module owns parameterized catalog write plans only. SPI execution stays in
//! `pg_koldstore`.
//!
//! Publication protocol: segments insert as `pending` with checksum/etag;
//! [`plan_activate_flush_segments`] CAS-bumps `manifest.generation` and flips
//! those rows to `active` in one statement.

use koldstore_catalog::SyncState;
use koldstore_common::SqlStatement;
use thiserror::Error;

use koldstore_manifest::ManifestAssemblyError;

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

/// Plans combined multi-row segment and normalized-stat inserts as `pending`.
///
/// Segment prune metadata lives in `koldstore.cold_segment_index`. Exact per-PK
/// catalog rows are not written. Manifest export reads the same index table —
/// there is no duplicated `column_stats` JSON on `cold_segments`.
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
        $12::text[],
        $13::text[],
        $14::integer[],
        $15::integer[]
    ) AS u(
        segment_id,
        path,
        batch_number,
        min_seq,
        max_seq,
        min_commit_seq,
        max_commit_seq,
        row_count,
        byte_size,
        schema_version,
        checksum,
        object_etag,
        row_group_count,
        row_group_offset
    )
),
inserted_segments AS (
    INSERT INTO koldstore.cold_segments (
        segment_id,
        table_oid,
        scope_key,
        path,
        batch_number,
        min_seq,
        max_seq,
        min_commit_seq,
        max_commit_seq,
        row_count,
        byte_size,
        schema_version,
        row_group_count,
        row_group_row_counts,
        row_group_min_seqs,
        row_group_max_seqs,
        status,
        checksum,
        object_etag
    )
    SELECT
        u.segment_id,
        $1::oid,
        '',
        u.path,
        u.batch_number,
        u.min_seq,
        u.max_seq,
        u.min_commit_seq,
        u.max_commit_seq,
        u.row_count,
        u.byte_size,
        u.schema_version,
        u.row_group_count,
        ($16::bigint[])[
            (u.row_group_offset + 1):
            (u.row_group_offset + u.row_group_count)
        ],
        ($17::bigint[])[
            (u.row_group_offset + 1):
            (u.row_group_offset + u.row_group_count)
        ],
        ($18::bigint[])[
            (u.row_group_offset + 1):
            (u.row_group_offset + u.row_group_count)
        ],
        'pending',
        u.checksum,
        NULLIF(u.object_etag, '')
    FROM segment_input u
    RETURNING segment_id, table_oid, scope_key
),
index_input AS (
    SELECT *
    FROM unnest(
        $19::uuid[],
        $20::smallint[],
        $21::oid[],
        $22::smallint[],
        $23::bytea[],
        $24::bytea[],
        $25::integer[],
        $26::integer[]
    ) AS i(
        segment_id,
        column_id,
        type_oid,
        codec_version,
        min_value,
        max_value,
        row_group_count,
        row_group_offset
    )
)
INSERT INTO koldstore.cold_segment_index (
    segment_id,
    table_oid,
    scope_key,
    column_id,
    type_oid,
    codec_version,
    min_value,
    max_value,
    row_group_min_values,
    row_group_max_values,
    row_group_null_counts
)
SELECT
    cs.segment_id,
    cs.table_oid,
    cs.scope_key,
    i.column_id,
    i.type_oid,
    i.codec_version,
    i.min_value,
    i.max_value,
    ($27::bytea[])[
        (i.row_group_offset + 1):
        (i.row_group_offset + i.row_group_count)
    ],
    ($28::bytea[])[
        (i.row_group_offset + 1):
        (i.row_group_offset + i.row_group_count)
    ],
    ($29::bigint[])[
        (i.row_group_offset + 1):
        (i.row_group_offset + i.row_group_count)
    ]
FROM inserted_segments cs
JOIN index_input i ON i.segment_id = cs.segment_id
ON CONFLICT (segment_id, column_id)
DO UPDATE SET
    table_oid = EXCLUDED.table_oid,
    scope_key = EXCLUDED.scope_key,
    type_oid = EXCLUDED.type_oid,
    codec_version = EXCLUDED.codec_version,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    row_group_min_values = EXCLUDED.row_group_min_values,
    row_group_max_values = EXCLUDED.row_group_max_values,
    row_group_null_counts = EXCLUDED.row_group_null_counts
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
/// - `$4` segment_count
/// - `$5` max_seq
/// - `$6` max_commit_seq
/// - `$7` pending segment id array
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
        NULL,
        $3::bigint,
        '{in_sync}',
        $4::integer,
        $5::bigint,
        $6::bigint,
        NULL,
        now()
    )
    ON CONFLICT (table_oid, scope_key)
    DO UPDATE SET
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
      AND segment_id = ANY($7::uuid[])
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
    fn flush_segment_insert_persists_packed_row_group_metadata() {
        let statement = plan_flush_segments_batch_insert().unwrap();
        assert!(statement.sql.contains("'pending'"));
        assert!(statement.sql.contains("checksum"));
        assert!(statement.sql.contains("object_etag"));
        assert!(statement.sql.contains("column_id"));
        assert!(statement.sql.contains("koldstore.cold_segment_index"));
        assert!(statement.sql.contains("codec_version"));
        assert!(statement.sql.contains("row_group_count"));
        assert!(statement.sql.contains("row_group_row_counts"));
        assert!(statement.sql.contains("row_group_min_seqs"));
        assert!(statement.sql.contains("row_group_max_seqs"));
        assert!(statement.sql.contains("row_group_min_values"));
        assert!(statement.sql.contains("row_group_max_values"));
        assert!(statement.sql.contains("row_group_null_counts"));
        assert!(statement.sql.contains("row_group_offset"));
        assert!(statement.sql.contains("($16::bigint[])"));
        assert!(statement.sql.contains("($27::bytea[])"));
        assert!(statement.sql.contains("($29::bigint[])"));
        assert!(!statement.sql.contains("bytea[][]"));
        assert!(!statement.sql.contains("bigint[][]"));
        assert!(statement
            .sql
            .contains("ON CONFLICT (segment_id, column_id)"));
        assert!(!statement.sql.contains("column_stats"));
        assert!(!statement.sql.contains("jsonb_each"));
        assert!(!statement.sql.contains("convert_to"));
        assert!(!statement.sql.contains("cold_segment_stats"));
        assert!(!statement.sql.contains("column_name"));
        assert!(!statement.sql.contains("attribute.attname"));
        assert!(!statement.sql.contains("'active'"));
    }

    #[test]
    fn activate_plan_uses_generation_cas() {
        let statement = plan_activate_flush_segments().unwrap();
        assert!(statement
            .sql
            .contains("WHERE koldstore.manifest.generation = $2::bigint"));
        assert!(statement.sql.contains("SET status = 'active'"));
        assert!(statement.sql.contains("segment_id = ANY($7::uuid[])"));
    }
}
