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
use koldstore_common::{ColumnId, SqlStatement};
use koldstore_sortkey::{encode_sort_key_json, SortKeyType, CODEC_VERSION};
use thiserror::Error;

pub use koldstore_catalog::CatalogManifestSegmentRow;
pub use koldstore_manifest::{
    build_manifest_segment_from_catalog_row, manifest_from_catalog_rows, write_manifest_to_path,
    ManifestAssemblyError,
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
    /// A supported indexed-column bound could not be encoded.
    #[error("{0}")]
    SortKey(String),
}

impl From<ManifestAssemblyError> for SegmentCatalogError {
    fn from(error: ManifestAssemblyError) -> Self {
        Self::Manifest(error.to_string())
    }
}

/// One Sort Key V1 index row ready for a typed SPI array insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedColumnBound {
    /// Stable PostgreSQL attribute number.
    pub column_id: ColumnId,
    /// PostgreSQL type OID used to interpret the encoded bytes.
    pub type_oid: u32,
    /// Persisted Sort Key codec version.
    pub codec_version: i16,
    /// Encoded inclusive lower segment bound.
    pub min_value: Vec<u8>,
    /// Encoded inclusive upper segment bound.
    pub max_value: Vec<u8>,
}

/// Encodes supported indexed-column JSON bounds as Sort Key V1 bytes.
///
/// Columns missing bounds or catalog type metadata are omitted. Types outside
/// the Sort Key V1 allowlist are intentionally skipped.
///
/// # Errors
///
/// Returns an error when a supported type's JSON bound has an invalid shape or
/// Storekey cannot encode it.
pub fn encode_indexed_column_bounds(
    indexed_bounds: &std::collections::BTreeMap<ColumnId, (serde_json::Value, serde_json::Value)>,
    type_oids: &std::collections::BTreeMap<ColumnId, u32>,
) -> Result<Vec<EncodedColumnBound>, SegmentCatalogError> {
    let mut encoded = Vec::with_capacity(indexed_bounds.len());
    for (column_id, bounds) in indexed_bounds {
        let Some(&type_oid) = type_oids.get(column_id) else {
            continue;
        };
        let Some(sort_key_type) = SortKeyType::from_type_oid(type_oid) else {
            continue;
        };
        let min_value = encode_sort_key_json(sort_key_type, &bounds.0)
            .map_err(|error| SegmentCatalogError::SortKey(error.to_string()))?;
        let max_value = encode_sort_key_json(sort_key_type, &bounds.1)
            .map_err(|error| SegmentCatalogError::SortKey(error.to_string()))?;
        encoded.push(EncodedColumnBound {
            column_id: *column_id,
            type_oid,
            codec_version: CODEC_VERSION,
            min_value,
            max_value,
        });
    }
    Ok(encoded)
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
        $13::text[]
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
        'pending',
        u.checksum,
        NULLIF(u.object_etag, '')
    FROM segment_input u
    RETURNING segment_id, table_oid, scope_key
),
index_input AS (
    SELECT *
    FROM unnest(
        $14::uuid[],
        $15::smallint[],
        $16::oid[],
        $17::smallint[],
        $18::bytea[],
        $19::bytea[]
    ) AS i(
        segment_id,
        column_id,
        type_oid,
        codec_version,
        min_value,
        max_value
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
    max_value
)
SELECT
    cs.segment_id,
    cs.table_oid,
    cs.scope_key,
    i.column_id,
    i.type_oid,
    i.codec_version,
    i.min_value,
    i.max_value
FROM inserted_segments cs
JOIN index_input i ON i.segment_id = cs.segment_id
ON CONFLICT (segment_id, column_id)
DO UPDATE SET
    table_oid = EXCLUDED.table_oid,
    scope_key = EXCLUDED.scope_key,
    type_oid = EXCLUDED.type_oid,
    codec_version = EXCLUDED.codec_version,
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
    use super::{
        encode_indexed_column_bounds, plan_activate_flush_segments,
        plan_flush_segments_batch_insert,
    };
    use koldstore_common::ColumnId;
    use koldstore_sortkey::{decode_sort_key, SortKeyType, SortKeyValue, CODEC_VERSION};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn indexed_bounds_encode_supported_types_and_skip_unsupported_types() {
        let bounds = BTreeMap::from([
            (ColumnId::from_attnum(1), (json!(-5), json!(42))),
            (ColumnId::from_attnum(2), (json!("a"), json!("z"))),
        ]);
        let type_oids = BTreeMap::from([
            (ColumnId::from_attnum(1), 20),
            (ColumnId::from_attnum(2), 25),
        ]);

        let encoded = encode_indexed_column_bounds(&bounds, &type_oids).unwrap();

        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0].column_id, ColumnId::from_attnum(1));
        assert_eq!(encoded[0].type_oid, 20);
        assert_eq!(encoded[0].codec_version, CODEC_VERSION);
        assert_eq!(
            decode_sort_key(SortKeyType::Int8, &encoded[0].min_value).unwrap(),
            SortKeyValue::Int8(-5)
        );
        assert_eq!(
            decode_sort_key(SortKeyType::Int8, &encoded[0].max_value).unwrap(),
            SortKeyValue::Int8(42)
        );
    }

    #[test]
    fn flush_segment_insert_plans_pending_with_checksum() {
        let statement = plan_flush_segments_batch_insert().unwrap();
        assert!(statement.sql.contains("'pending'"));
        assert!(statement.sql.contains("checksum"));
        assert!(statement.sql.contains("object_etag"));
        assert!(statement.sql.contains("column_id"));
        assert!(statement.sql.contains("koldstore.cold_segment_index"));
        assert!(statement.sql.contains("codec_version"));
        assert!(statement.sql.contains("$18::bytea[]"));
        assert!(statement.sql.contains("$19::bytea[]"));
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
        assert!(statement.sql.contains("segment_id = ANY($8::uuid[])"));
    }
}
