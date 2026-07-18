//! Pure decoders for catalog JSON payloads.

use std::collections::BTreeMap;

/// PostgreSQL relation identity resolved from `pg_class`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationContext {
    /// Schema name.
    pub namespace: String,
    /// Relation name.
    pub name: String,
}

/// Active cold-segment stats row returned by merge-scan catalog lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestScanSegmentStats {
    /// Final object-store path.
    pub object_path: String,
    /// Segment-level min/max stats by column.
    pub column_stats: serde_json::Value,
    /// Object byte size when known (enables bounded footer range GETs).
    pub byte_size: Option<u64>,
}

/// Manifest-backed merge-scan context loaded from catalog SPI.
///
/// Named historically for the flush `in_sync` publish path; callers also load
/// this after hot DML leaves `sync_state = pending_write` while cold remains
/// readable from the last published generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InSyncManifestScanContext {
    /// Latest published manifest object path.
    pub manifest_path: String,
    /// Monotonic catalog generation (CAS identity).
    pub generation: u64,
    /// Object-store base path for the managed table.
    pub base_path: String,
    /// Catalog storage backend type (`filesystem`, `s3`, …).
    pub storage_type: String,
    /// Storage credentials JSON (may be empty for filesystem).
    pub credentials: serde_json::Value,
    /// Storage backend config JSON.
    pub config: serde_json::Value,
    /// Active shared-scope cold segments ordered by batch number.
    pub segments: Vec<ManifestScanSegmentStats>,
}

/// Storage context required to publish a flush segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushStorageContext {
    /// Object-store base path.
    pub base_path: String,
    /// Catalog storage backend type (`filesystem`, `s3`, …).
    pub storage_type: String,
    /// Storage credentials JSON (may be empty for filesystem).
    pub credentials: serde_json::Value,
    /// Storage backend config JSON.
    pub config: serde_json::Value,
    /// Active KoldStore schema version.
    pub schema_version: i32,
    /// Configured Parquet compression codec.
    pub compression: String,
}

/// Decodes a relation context JSON payload.
///
/// # Errors
///
/// Returns an error when required fields are missing or have the wrong type.
pub fn relation_context(value: &serde_json::Value) -> Result<RelationContext, String> {
    Ok(RelationContext {
        namespace: required_string(value, "namespace")?.to_string(),
        name: required_string(value, "name")?.to_string(),
    })
}

/// Decodes a published manifest scan context JSON payload.
///
/// # Errors
///
/// Returns an error when required fields are missing or have the wrong type.
pub fn in_sync_manifest_scan_context(
    value: &serde_json::Value,
) -> Result<InSyncManifestScanContext, String> {
    let segments = value
        .get("segments")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "missing array field `segments`".to_string())?
        .iter()
        .map(manifest_scan_segment_stats)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(InSyncManifestScanContext {
        manifest_path: required_string(value, "manifest_path")?.to_string(),
        generation: required_u64(value, "generation")?,
        base_path: required_string(value, "base_path")?.to_string(),
        storage_type: optional_string(value, "storage_type")
            .unwrap_or("filesystem")
            .to_string(),
        credentials: value
            .get("credentials")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
        config: value
            .get("config")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
        segments,
    })
}

/// Decodes one manifest scan segment stats row.
///
/// # Errors
///
/// Returns an error when required fields are missing or have the wrong type.
pub fn manifest_scan_segment_stats(
    value: &serde_json::Value,
) -> Result<ManifestScanSegmentStats, String> {
    let column_stats = value
        .get("column_stats")
        .cloned()
        .ok_or_else(|| "missing field `column_stats`".to_string())?;

    Ok(ManifestScanSegmentStats {
        object_path: required_string(value, "object_path")?.to_string(),
        column_stats,
        byte_size: optional_u64(value, "byte_size"),
    })
}

/// Extracts `{column: (min, max)}` pairs from catalog column-stats JSON.
///
/// Columns missing either `min` or `max` are skipped. Used by manifest assembly
/// and merge-scan segment pruning so both paths share one walk.
#[must_use]
pub fn column_stats_min_max_map(
    column_stats: &serde_json::Value,
) -> BTreeMap<String, (serde_json::Value, serde_json::Value)> {
    let mut stats = BTreeMap::new();
    let Some(columns) = column_stats.as_object() else {
        return stats;
    };
    for (column, value) in columns {
        let Some(min) = value.get("min") else {
            continue;
        };
        let Some(max) = value.get("max") else {
            continue;
        };
        stats.insert(column.clone(), (min.clone(), max.clone()));
    }
    stats
}

/// Like [`column_stats_min_max_map`], but takes ownership and moves min/max out.
#[must_use]
pub fn column_stats_min_max_map_into(
    column_stats: serde_json::Value,
) -> BTreeMap<String, (serde_json::Value, serde_json::Value)> {
    let mut stats = BTreeMap::new();
    let serde_json::Value::Object(columns) = column_stats else {
        return stats;
    };
    for (column, value) in columns {
        let serde_json::Value::Object(mut bounds) = value else {
            continue;
        };
        let Some(min) = bounds.remove("min") else {
            continue;
        };
        let Some(max) = bounds.remove("max") else {
            continue;
        };
        stats.insert(column, (min, max));
    }
    stats
}

/// Decodes a flush storage context JSON payload.
///
/// # Errors
///
/// Returns an error when required fields are missing or have the wrong type.
pub fn flush_storage_context(value: &serde_json::Value) -> Result<FlushStorageContext, String> {
    let schema_version = required_i32(value, "schema_version")?;
    Ok(FlushStorageContext {
        base_path: required_string(value, "base_path")?.to_string(),
        storage_type: optional_string(value, "storage_type")
            .unwrap_or("filesystem")
            .to_string(),
        credentials: value
            .get("credentials")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
        config: value
            .get("config")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
        schema_version,
        compression: required_string(value, "compression")?.to_string(),
    })
}

fn required_string<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("missing string field `{field}`"))
}

fn optional_string<'a>(value: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

fn required_i32(value: &serde_json::Value, field: &str) -> Result<i32, String> {
    let raw = value
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| format!("missing integer field `{field}`"))?;
    i32::try_from(raw).map_err(|error| error.to_string())
}

fn required_u64(value: &serde_json::Value, field: &str) -> Result<u64, String> {
    let raw = value
        .get(field)
        .ok_or_else(|| format!("missing field `{field}`"))?;
    if let Some(number) = raw.as_u64() {
        return Ok(number);
    }
    if let Some(number) = raw.as_i64() {
        return u64::try_from(number).map_err(|error| error.to_string());
    }
    if let Some(text) = raw.as_str() {
        return text
            .parse::<u64>()
            .map_err(|_| format!("field `{field}` must be an integer generation"));
    }
    Err(format!("field `{field}` must be an integer"))
}

fn optional_u64(value: &serde_json::Value, field: &str) -> Option<u64> {
    value.get(field).and_then(|raw| {
        raw.as_u64()
            .or_else(|| raw.as_i64().and_then(|n| u64::try_from(n).ok()))
    })
}

#[cfg(test)]
mod tests {
    use super::{column_stats_min_max_map, flush_storage_context, in_sync_manifest_scan_context};

    #[test]
    fn in_sync_manifest_scan_context_decodes_segment_stats() {
        let value = serde_json::json!({
            "manifest_path": "ns/table/manifest.json",
            "generation": 3,
            "base_path": "/tmp/koldstore",
            "segments": [
                {
                    "object_path": "ns/table/batch-1.parquet",
                    "column_stats": {"seq": {"min": 1, "max": 100}}
                }
            ]
        });

        let context = in_sync_manifest_scan_context(&value).unwrap();
        assert_eq!(context.manifest_path, "ns/table/manifest.json");
        assert_eq!(context.generation, 3);
        assert_eq!(context.base_path, "/tmp/koldstore");
        assert_eq!(context.storage_type, "filesystem");
        assert_eq!(context.segments.len(), 1);
        assert_eq!(context.segments[0].object_path, "ns/table/batch-1.parquet");
        assert_eq!(context.segments[0].byte_size, None);
        assert_eq!(
            context.segments[0].column_stats,
            serde_json::json!({"seq": {"min": 1, "max": 100}})
        );
    }

    #[test]
    fn in_sync_manifest_scan_context_decodes_byte_size() {
        let value = serde_json::json!({
            "manifest_path": "ns/table/manifest.json",
            "generation": 3,
            "base_path": "s3://bucket/prefix",
            "storage_type": "s3",
            "segments": [
                {
                    "object_path": "ns/table/batch-1.parquet",
                    "column_stats": {"id": {"min": 1, "max": 10}},
                    "byte_size": 4096
                }
            ]
        });
        let context = in_sync_manifest_scan_context(&value).unwrap();
        assert_eq!(context.segments[0].byte_size, Some(4096));
    }

    #[test]
    fn column_stats_min_max_map_skips_incomplete_bounds() {
        let value = serde_json::json!({
            "seq": {"min": 1, "max": 100},
            "partial": {"min": 1},
            "other": {"max": 9}
        });
        let stats = column_stats_min_max_map(&value);
        assert_eq!(stats.len(), 1);
        assert_eq!(
            stats.get("seq"),
            Some(&(serde_json::json!(1), serde_json::json!(100)))
        );
    }

    #[test]
    fn flush_storage_context_requires_compression_codec() {
        let value = serde_json::json!({
            "base_path": "/tmp/koldstore",
            "schema_version": 3,
            "compression": "zstd"
        });

        let context = flush_storage_context(&value).unwrap();
        assert_eq!(context.base_path, "/tmp/koldstore");
        assert_eq!(context.storage_type, "filesystem");
        assert_eq!(context.schema_version, 3);
        assert_eq!(context.compression, "zstd");
    }

    #[test]
    fn flush_storage_context_decodes_s3_fields() {
        let value = serde_json::json!({
            "base_path": "s3://koldstore-test/prefix",
            "storage_type": "s3",
            "credentials": {"access_key_id": "a", "secret_access_key": "b"},
            "config": {"endpoint": "http://127.0.0.1:19000", "path_style": true},
            "schema_version": 1,
            "compression": "zstd"
        });
        let context = flush_storage_context(&value).unwrap();
        assert_eq!(context.storage_type, "s3");
        assert_eq!(context.config["path_style"], true);
    }
}
