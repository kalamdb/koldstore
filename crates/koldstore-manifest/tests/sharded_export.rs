use koldstore_manifest::{
    relative_manifest_path, try_load_manifest_with_client, write_manifest_with_client, Manifest,
    ManifestSegment, MANIFEST_VERSION,
};
use koldstore_storage::{
    content_checksum_sha256_hex, open_filesystem_client, publish_mutable_object, StorageClient,
};

fn one_segment_manifest() -> Manifest {
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
    manifest
}

#[test]
fn sharded_write_and_load_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let client = open_filesystem_client(dir.path().to_str().unwrap()).unwrap();
    let root_key = relative_manifest_path("app", "items");

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
        200,
        1,
    ));
    write_manifest_with_client(&client, &root_key, &manifest).unwrap();

    assert!(dir.path().join("app/items/manifest.json").is_file());

    let root_json = std::fs::read_to_string(dir.path().join("app/items/manifest.json")).unwrap();
    assert!(!root_json.contains("segment-0001"));
    let root_value: serde_json::Value = serde_json::from_str(&root_json).unwrap();
    assert_eq!(
        root_value["shards"][0]["content_sha256"]
            .as_str()
            .map(str::len),
        Some(64)
    );
    for shard in root_value["shards"].as_array().unwrap() {
        let path = shard["path"].as_str().unwrap();
        let hash = shard["content_sha256"].as_str().unwrap();
        assert!(
            path.contains(hash),
            "shard path is not content-addressed: {path}"
        );
        assert!(dir.path().join("app/items").join(path).is_file());
    }

    let loaded = try_load_manifest_with_client(&client, &root_key)
        .unwrap()
        .expect("export should load");
    assert_eq!(loaded.version, MANIFEST_VERSION);
    assert_eq!(loaded.shards.len(), 2);
    assert_eq!(loaded.segments.len(), 2);
    assert_eq!(loaded.max_seq, 20);
    assert_eq!(loaded.segments[0].path, "001/segment-0001-aaaaaaaa.parquet");
    assert_eq!(loaded.segments[1].path, "002/segment-0101-bbbbbbbb.parquet");
}

#[test]
fn root_with_embedded_segments_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let client = open_filesystem_client(dir.path().to_str().unwrap()).unwrap();
    let root_key = relative_manifest_path("app", "notes");

    let mut monolith = Manifest::new_shared("app", "notes", 1);
    monolith.append_segment(ManifestSegment::committed(
        1,
        "001/segment-0001-aaaaaaaa.parquet",
        5..=5,
        5..=5,
        1,
        32,
        1,
    ));
    // Bypass split and put a root that still embeds segments.
    let bytes = koldstore_manifest::manifest_to_json_bytes(&monolith).unwrap();
    koldstore_storage::publish_mutable_object(&client, &root_key, &bytes).unwrap();

    let err = try_load_manifest_with_client(&client, &root_key).unwrap_err();
    assert!(
        err.contains("must not embed segments"),
        "unexpected error: {err}"
    );
}

#[test]
fn root_without_shards_field_is_rejected() {
    let client = koldstore_storage::ObjectStoreClient::in_memory();
    let root_key = relative_manifest_path("app", "items");
    let mut root = serde_json::to_value(Manifest::new_shared("app", "items", 1)).unwrap();
    root.as_object_mut().unwrap().remove("shards");
    publish_mutable_object(&client, &root_key, &serde_json::to_vec(&root).unwrap()).unwrap();

    let error = try_load_manifest_with_client(&client, &root_key).unwrap_err();
    assert!(error.contains("shards"), "unexpected error: {error}");
}

#[test]
fn unsupported_root_version_is_rejected() {
    let client = koldstore_storage::ObjectStoreClient::in_memory();
    let root_key = relative_manifest_path("app", "items");
    let mut root = serde_json::to_value(Manifest::new_shared("app", "items", 1)).unwrap();
    root["version"] = serde_json::json!(3);
    publish_mutable_object(&client, &root_key, &serde_json::to_vec(&root).unwrap()).unwrap();

    let error = try_load_manifest_with_client(&client, &root_key).unwrap_err();
    assert!(
        error.contains("unsupported root manifest version"),
        "unexpected error: {error}"
    );
}

#[test]
fn shard_checksum_mismatch_is_rejected() {
    let client = koldstore_storage::ObjectStoreClient::in_memory();
    let root_key = relative_manifest_path("app", "items");
    write_manifest_with_client(&client, &root_key, &one_segment_manifest()).unwrap();

    let root: serde_json::Value = serde_json::from_slice(&client.get(&root_key).unwrap()).unwrap();
    let shard_key = format!("app/items/{}", root["shards"][0]["path"].as_str().unwrap());
    let mut shard: serde_json::Value =
        serde_json::from_slice(&client.get(&shard_key).unwrap()).unwrap();
    shard["folder"] = serde_json::json!("002");
    publish_mutable_object(&client, &shard_key, &serde_json::to_vec(&shard).unwrap()).unwrap();

    let error = try_load_manifest_with_client(&client, &root_key).unwrap_err();
    assert!(
        error.contains("content checksum mismatch"),
        "unexpected error: {error}"
    );
}

#[test]
fn shard_metadata_mismatch_is_rejected_after_checksum_verification() {
    let client = koldstore_storage::ObjectStoreClient::in_memory();
    let root_key = relative_manifest_path("app", "items");
    write_manifest_with_client(&client, &root_key, &one_segment_manifest()).unwrap();

    let mut root: serde_json::Value =
        serde_json::from_slice(&client.get(&root_key).unwrap()).unwrap();
    let old_shard_key = format!("app/items/{}", root["shards"][0]["path"].as_str().unwrap());
    let mut shard: serde_json::Value =
        serde_json::from_slice(&client.get(&old_shard_key).unwrap()).unwrap();
    shard["folder"] = serde_json::json!("002");
    let shard_bytes = serde_json::to_vec(&shard).unwrap();
    let shard_hash = content_checksum_sha256_hex(&shard_bytes);
    let shard_path = koldstore_manifest::relative_manifest_shard_content_path(1, &shard_hash);
    publish_mutable_object(&client, &format!("app/items/{shard_path}"), &shard_bytes).unwrap();

    root["shards"][0]["content_sha256"] = serde_json::json!(shard_hash);
    root["shards"][0]["path"] = serde_json::json!(shard_path);
    publish_mutable_object(&client, &root_key, &serde_json::to_vec(&root).unwrap()).unwrap();

    let error = try_load_manifest_with_client(&client, &root_key).unwrap_err();
    assert!(
        error.contains("folder does not match root reference"),
        "unexpected error: {error}"
    );
}

#[test]
fn segment_batch_must_match_its_folder() {
    let client = koldstore_storage::ObjectStoreClient::in_memory();
    let root_key = relative_manifest_path("app", "items");
    let mut manifest = Manifest::new_shared("app", "items", 1);
    manifest.append_segment(ManifestSegment::committed(
        101,
        "001/segment-0101-aaaaaaaa.parquet",
        1..=10,
        1..=10,
        10,
        100,
        1,
    ));

    let error = write_manifest_with_client(&client, &root_key, &manifest).unwrap_err();
    assert!(
        error.contains("batch 101 belongs in folder 002"),
        "unexpected error: {error}"
    );
}

#[test]
fn malformed_packed_manifest_arrays_are_rejected_before_publish() {
    let client = koldstore_storage::ObjectStoreClient::in_memory();
    let root_key = relative_manifest_path("app", "items");
    let mut manifest = one_segment_manifest();
    manifest.segments[0].row_group_count = 2;

    let error = write_manifest_with_client(&client, &root_key, &manifest).unwrap_err();
    assert!(error.contains("row-group array cardinality"));
    assert!(client.get(&root_key).is_err());
}
