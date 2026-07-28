//! Split a full in-memory [`Manifest`] into a thin root plus folder shards.
//!
//! Query prune never reads these files; they are a derived object-store export
//! for recovery, EXPORT, and kalamdb-compatible tooling.

use std::collections::BTreeMap;

use koldstore_storage::content_checksum_sha256_hex;

use crate::model::{
    FilesState, Manifest, ManifestShard, ManifestShardRef, MANIFEST_SHARD_VERSION, MANIFEST_VERSION,
};
use crate::paths::{
    folder_from_segment_relative_path, parse_folder_name, relative_manifest_shard_path_for_folder,
    segment_folder_number, SEGMENTS_PER_FOLDER,
};

/// Thin root plus table-relative shard documents ready for object put.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ShardedManifestExport {
    /// Root `manifest.json` body (`shards`, empty `segments`).
    pub(crate) root: Manifest,
    /// `(table-relative path, shard document)` pairs, ordered by folder.
    pub(crate) shards: Vec<(String, ManifestShard)>,
}

/// Splits an assembled manifest into the on-disk folder-sharded layout.
///
/// Groups segments by the leading folder component of each relative path
/// (`001/segment-….parquet`). Empty manifests still produce a versioned root
/// with an empty `shards` list.
///
/// # Errors
///
/// Returns an error when a segment path has no folder component.
pub(crate) fn split_manifest_for_export(
    manifest: &Manifest,
) -> Result<ShardedManifestExport, String> {
    let mut by_folder: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for segment in &manifest.segments {
        let folder = validated_segment_folder(segment)?.to_string();
        by_folder.entry(folder).or_default().push(segment.clone());
    }

    let mut shard_refs = Vec::with_capacity(by_folder.len());
    let mut shard_docs = Vec::with_capacity(by_folder.len());
    let mut max_folder_num = 0u32;

    for (folder, segments) in by_folder {
        let folder_num = parse_folder_name(&folder)
            .ok_or_else(|| format!("invalid manifest segment folder: {folder}"))?;
        max_folder_num = max_folder_num.max(folder_num);
        let path = relative_manifest_shard_path_for_folder(folder_num);
        let min_seq = segments.iter().map(|s| s.min_seq).min().unwrap_or(0);
        let max_seq = segments.iter().map(|s| s.max_seq).max().unwrap_or(0);
        let min_commit_seq = segments.iter().map(|s| s.min_commit_seq).min().unwrap_or(0);
        let max_commit_seq = segments.iter().map(|s| s.max_commit_seq).max().unwrap_or(0);
        let segment_count = u32::try_from(segments.len()).map_err(|error| error.to_string())?;

        let shard = ManifestShard {
            version: MANIFEST_SHARD_VERSION,
            folder: folder.clone(),
            table: manifest.table.clone(),
            namespace: manifest.namespace.clone(),
            schema_version: manifest.schema_version,
            segments,
        };
        let shard_bytes = serde_json::to_vec(&shard).map_err(|error| error.to_string())?;
        shard_refs.push(ManifestShardRef {
            folder: folder.clone(),
            path: path.clone(),
            content_sha256: content_checksum_sha256_hex(&shard_bytes),
            segment_count,
            min_seq,
            max_seq,
            min_commit_seq,
            max_commit_seq,
        });
        shard_docs.push((path, shard));
    }

    let total_files = u64::try_from(manifest.segments.len()).unwrap_or(u64::MAX);
    let mut root = manifest.clone();
    root.version = MANIFEST_VERSION;
    root.shards = shard_refs;
    root.segments.clear();
    root.files = FilesState {
        current_subfolder: if max_folder_num == 0 {
            "001".to_string()
        } else {
            format!("{max_folder_num:03}")
        },
        subfolder_count: u32::try_from(root.shards.len()).unwrap_or(u32::MAX),
        max_files_per_subfolder: SEGMENTS_PER_FOLDER,
        total_files: Some(total_files),
    };

    Ok(ShardedManifestExport {
        root,
        shards: shard_docs,
    })
}

/// Validates and merges shard documents into a root.
///
/// Preserves root watermarks/publish metadata and replaces `segments` with the
/// concatenation of shard segments in shard-list order.
///
/// # Errors
///
/// Returns an error when root, reference, or shard metadata disagree.
pub(crate) fn merge_manifest_shards(
    mut root: Manifest,
    shards: Vec<ManifestShard>,
) -> Result<Manifest, String> {
    validate_root(&root, shards.len())?;
    let capacity = root.shards.iter().try_fold(0usize, |total, shard| {
        usize::try_from(shard.segment_count)
            .ok()
            .and_then(|count| total.checked_add(count))
            .ok_or_else(|| "manifest segment count exceeds platform capacity".to_string())
    })?;
    let mut segments = Vec::with_capacity(capacity);
    for (shard_ref, shard) in root.shards.iter().zip(shards) {
        validate_shard(&root, shard_ref, &shard)?;
        segments.extend(shard.segments);
    }
    root.segments = segments;
    Ok(root)
}

fn validate_root(root: &Manifest, shard_count: usize) -> Result<(), String> {
    if root.version != MANIFEST_VERSION {
        return Err(format!(
            "unsupported root manifest version {}; expected {MANIFEST_VERSION}",
            root.version
        ));
    }
    if !root.segments.is_empty() {
        return Err(
            "root manifest must not embed segments; expected folder-sharded layout".to_string(),
        );
    }
    if root.shards.len() != shard_count {
        return Err(format!(
            "root lists {} shards but {shard_count} were loaded",
            root.shards.len()
        ));
    }
    Ok(())
}

fn validate_shard(
    root: &Manifest,
    shard_ref: &ManifestShardRef,
    shard: &ManifestShard,
) -> Result<(), String> {
    let folder_number = parse_folder_name(&shard_ref.folder)
        .ok_or_else(|| format!("invalid shard folder: {}", shard_ref.folder))?;
    let expected_path = relative_manifest_shard_path_for_folder(folder_number);
    if shard_ref.path != expected_path {
        return Err(format!(
            "shard path {} does not match folder {}",
            shard_ref.path, shard_ref.folder
        ));
    }
    if shard.version != MANIFEST_SHARD_VERSION {
        return Err(format!(
            "unsupported shard manifest version {}; expected {MANIFEST_SHARD_VERSION}",
            shard.version
        ));
    }
    if shard.folder != shard_ref.folder {
        return Err(format!(
            "shard folder does not match root reference: expected {}, got {}",
            shard_ref.folder, shard.folder
        ));
    }
    if shard.table != root.table
        || shard.namespace != root.namespace
        || shard.schema_version != root.schema_version
    {
        return Err(format!(
            "shard {} table metadata does not match root",
            shard_ref.path
        ));
    }
    validate_shard_segments(shard_ref, shard)
}

fn validate_shard_segments(
    shard_ref: &ManifestShardRef,
    shard: &ManifestShard,
) -> Result<(), String> {
    let actual_count = u32::try_from(shard.segments.len())
        .map_err(|_| format!("shard {} contains too many segments", shard_ref.path))?;
    if actual_count != shard_ref.segment_count {
        return Err(format!(
            "shard {} segment count mismatch: expected {}, got {actual_count}",
            shard_ref.path, shard_ref.segment_count
        ));
    }
    for segment in &shard.segments {
        let folder = validated_segment_folder(segment)?;
        if folder != shard.folder {
            return Err(format!(
                "segment {} does not belong to shard folder {}",
                segment.path, shard.folder
            ));
        }
    }
    let ranges = shard_ranges(&shard.segments);
    let expected = (
        shard_ref.min_seq,
        shard_ref.max_seq,
        shard_ref.min_commit_seq,
        shard_ref.max_commit_seq,
    );
    if ranges != expected {
        return Err(format!("shard {} range metadata mismatch", shard_ref.path));
    }
    Ok(())
}

fn validated_segment_folder(segment: &crate::model::ManifestSegment) -> Result<&str, String> {
    let folder = folder_from_segment_relative_path(&segment.path)
        .ok_or_else(|| format!("invalid manifest segment path: {}", segment.path))?;
    let folder_number = parse_folder_name(folder)
        .ok_or_else(|| format!("invalid manifest segment folder: {folder}"))?;
    let batch = i32::try_from(segment.batch)
        .map_err(|_| format!("manifest segment batch is too large: {}", segment.batch))?;
    let expected_folder = segment_folder_number(batch);
    if expected_folder != folder_number {
        return Err(format!(
            "manifest segment batch {} belongs in folder {expected_folder:03}, not {folder}",
            segment.batch
        ));
    }
    Ok(folder)
}

fn shard_ranges(segments: &[crate::model::ManifestSegment]) -> (i64, i64, i64, i64) {
    (
        segments
            .iter()
            .map(|segment| segment.min_seq)
            .min()
            .unwrap_or(0),
        segments
            .iter()
            .map(|segment| segment.max_seq)
            .max()
            .unwrap_or(0),
        segments
            .iter()
            .map(|segment| segment.min_commit_seq)
            .min()
            .unwrap_or(0),
        segments
            .iter()
            .map(|segment| segment.max_commit_seq)
            .max()
            .unwrap_or(0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ManifestSegment;

    #[test]
    fn split_groups_by_folder_and_clears_root_segments() {
        let mut manifest = Manifest::new_shared("app", "items", 1);
        manifest.append_segment(ManifestSegment::committed(
            1,
            "001/segment-0001-aaaaaaaa.parquet",
            1..=10,
            1..=10,
            10,
            100,
            1,
        ));
        manifest.append_segment(ManifestSegment::committed(
            101,
            "002/segment-0101-bbbbbbbb.parquet",
            11..=20,
            11..=20,
            10,
            100,
            1,
        ));

        let export = split_manifest_for_export(&manifest).unwrap();
        assert_eq!(export.root.version, MANIFEST_VERSION);
        assert!(export.root.segments.is_empty());
        assert_eq!(export.shards.len(), 2);
        assert_eq!(export.root.shards[0].folder, "001");
        assert_eq!(export.root.shards[0].path, "001/manifest-shard.json");
        assert_eq!(export.root.shards[0].segment_count, 1);
        assert_eq!(export.root.shards[1].folder, "002");
        assert_eq!(export.root.files.current_subfolder, "002");
        assert_eq!(export.root.files.subfolder_count, 2);
        assert_eq!(export.root.files.total_files, Some(2));

        let merged = merge_manifest_shards(
            export.root.clone(),
            export.shards.into_iter().map(|(_, shard)| shard).collect(),
        )
        .unwrap();
        assert_eq!(merged.segments.len(), 2);
        assert_eq!(merged.max_seq, 20);
    }
}
