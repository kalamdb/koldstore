use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};
use std::thread;

use koldstore_manifest::{
    relative_manifest_path, try_load_manifest_with_client, write_manifest_with_client, Manifest,
    ManifestSegment, MANIFEST_VERSION,
};
use koldstore_storage::{
    content_checksum_sha256_hex, open_filesystem_client, publish_mutable_object, ObjectStoreClient,
    PutOutcome, PutPrecondition, StorageClient, StorageClientError, StorageObject, StorageResult,
};

struct StaleRootOnceClient {
    inner: ObjectStoreClient,
    root_key: String,
    stale_root: Vec<u8>,
    served_stale_root: AtomicBool,
}

struct CoordinatedRootReader {
    inner: ObjectStoreClient,
    root_key: String,
    start_read: Arc<Barrier>,
    finish_publish: Arc<Barrier>,
    gated: AtomicBool,
}

impl StorageClient for CoordinatedRootReader {
    fn list(&self, prefix: &str) -> StorageResult<Vec<StorageObject>> {
        self.inner.list(prefix)
    }

    fn put(&self, key: &str, bytes: &[u8], mode: PutPrecondition) -> StorageResult<PutOutcome> {
        self.inner.put(key, bytes, mode)
    }

    fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
        let bytes = self.inner.get(key)?;
        if key == self.root_key && !self.gated.swap(true, Ordering::SeqCst) {
            self.start_read.wait();
            self.finish_publish.wait();
        }
        Ok(bytes)
    }

    fn head(&self, key: &str) -> StorageResult<StorageObject> {
        self.inner.head(key)
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        self.inner.delete(key)
    }

    fn copy_if_absent(&self, from: &str, to: &str) -> StorageResult<()> {
        self.inner.copy_if_absent(from, to)
    }
}

impl StorageClient for StaleRootOnceClient {
    fn list(&self, prefix: &str) -> StorageResult<Vec<StorageObject>> {
        self.inner.list(prefix)
    }

    fn put(&self, key: &str, bytes: &[u8], mode: PutPrecondition) -> StorageResult<PutOutcome> {
        self.inner.put(key, bytes, mode)
    }

    fn get(&self, key: &str) -> StorageResult<Vec<u8>> {
        if key == self.root_key && !self.served_stale_root.swap(true, Ordering::SeqCst) {
            return Ok(self.stale_root.clone());
        }
        self.inner.get(key)
    }

    fn head(&self, key: &str) -> StorageResult<StorageObject> {
        self.inner.head(key)
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        self.inner.delete(key)
    }

    fn copy_if_absent(&self, from: &str, to: &str) -> StorageResult<()> {
        self.inner.copy_if_absent(from, to)
    }
}

fn one_segment_manifest() -> Manifest {
    let mut manifest = Manifest::new_shared("app", "items", 1);
    manifest.append_segment(ManifestSegment::committed(
        1,
        "001/segment-0001-aaaaaaaa.parquet",
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
        10,
        100,
        1,
    ));
    manifest.append_segment(ManifestSegment::committed(
        101,
        "002/segment-0101-bbbbbbbb.parquet",
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
        let file_name = path.rsplit('/').next().unwrap();
        let token = file_name
            .strip_prefix("manifest-shard-")
            .and_then(|name| name.strip_suffix(".json"))
            .expect("content-addressed shard filename");
        assert_eq!(token.len(), 32, "shard filename token must be 128 bits");
        assert_eq!(token, &hash[..32]);
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
fn republish_removes_unreferenced_shards_and_keeps_segments() {
    let dir = tempfile::tempdir().unwrap();
    let client = open_filesystem_client(dir.path().to_str().unwrap()).unwrap();
    let root_key = relative_manifest_path("app", "items");
    let mut manifest = one_segment_manifest();

    write_manifest_with_client(&client, &root_key, &manifest).unwrap();
    let first_root: serde_json::Value =
        serde_json::from_slice(&client.get(&root_key).unwrap()).unwrap();
    let first_shard = format!(
        "app/items/{}",
        first_root["shards"][0]["path"].as_str().unwrap()
    );

    let legacy_shard = format!("app/items/001/manifest-shard-{}.json", "f".repeat(64));
    client
        .put(&legacy_shard, b"legacy", PutPrecondition::CreateIfAbsent)
        .unwrap();
    let segment_key = "app/items/001/segment-0001-aaaaaaaa.parquet";
    client
        .put(segment_key, b"parquet", PutPrecondition::CreateIfAbsent)
        .unwrap();

    manifest.append_segment(ManifestSegment::committed(
        2,
        "001/segment-0002-bbbbbbbb.parquet",
        11..=20,
        10,
        100,
        1,
    ));
    write_manifest_with_client(&client, &root_key, &manifest).unwrap();

    let current_root: serde_json::Value =
        serde_json::from_slice(&client.get(&root_key).unwrap()).unwrap();
    let current_shard = format!(
        "app/items/{}",
        current_root["shards"][0]["path"].as_str().unwrap()
    );
    assert_ne!(first_shard, current_shard);
    assert!(matches!(
        client.get(&first_shard),
        Err(StorageClientError::NotFound { .. })
    ));
    assert!(matches!(
        client.get(&legacy_shard),
        Err(StorageClientError::NotFound { .. })
    ));
    assert!(client.get(&current_shard).is_ok());
    assert_eq!(client.get(segment_key).unwrap(), b"parquet");

    let shard_keys = client
        .list("app/items/001")
        .unwrap()
        .into_iter()
        .map(|object| object.key)
        .filter(|key| key.contains("/manifest-shard-") && key.ends_with(".json"))
        .collect::<Vec<_>>();
    assert_eq!(shard_keys, vec![current_shard]);
}

#[test]
fn load_retries_when_cleanup_removes_a_shard_from_a_stale_root() {
    let client = ObjectStoreClient::in_memory();
    let root_key = relative_manifest_path("app", "items");
    let mut manifest = one_segment_manifest();
    write_manifest_with_client(&client, &root_key, &manifest).unwrap();
    let stale_root = client.get(&root_key).unwrap();

    manifest.append_segment(ManifestSegment::committed(
        2,
        "001/segment-0002-bbbbbbbb.parquet",
        11..=20,
        10,
        100,
        1,
    ));
    write_manifest_with_client(&client, &root_key, &manifest).unwrap();

    let racing_client = StaleRootOnceClient {
        inner: client,
        root_key: root_key.clone(),
        stale_root,
        served_stale_root: AtomicBool::new(false),
    };
    let loaded = try_load_manifest_with_client(&racing_client, &root_key)
        .unwrap()
        .expect("current manifest should load after retry");
    assert_eq!(loaded.segments.len(), 2);
    assert_eq!(loaded.max_seq, 20);
}

#[test]
fn five_concurrent_readers_survive_manifest_republish_and_shard_cleanup() {
    let client = ObjectStoreClient::in_memory();
    let root_key = relative_manifest_path("app", "items");
    let mut next_manifest = one_segment_manifest();
    write_manifest_with_client(&client, &root_key, &next_manifest).unwrap();

    let reader_count = 5;
    let start_read = Arc::new(Barrier::new(reader_count + 1));
    let finish_publish = Arc::new(Barrier::new(reader_count + 1));
    let readers = (0..reader_count)
        .map(|_| {
            let reader = CoordinatedRootReader {
                inner: client.clone(),
                root_key: root_key.clone(),
                start_read: Arc::clone(&start_read),
                finish_publish: Arc::clone(&finish_publish),
                gated: AtomicBool::new(false),
            };
            let root_key = root_key.clone();
            thread::spawn(move || {
                try_load_manifest_with_client(&reader, &root_key)
                    .unwrap()
                    .expect("manifest should remain readable during republish")
            })
        })
        .collect::<Vec<_>>();

    start_read.wait();
    next_manifest.append_segment(ManifestSegment::committed(
        2,
        "001/segment-0002-bbbbbbbb.parquet",
        11..=20,
        10,
        100,
        1,
    ));
    write_manifest_with_client(&client, &root_key, &next_manifest).unwrap();
    finish_publish.wait();

    for reader in readers {
        let loaded = reader.join().expect("reader thread should not panic");
        assert_eq!(loaded.segments.len(), 2);
        assert_eq!(loaded.max_seq, 20);
    }
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
