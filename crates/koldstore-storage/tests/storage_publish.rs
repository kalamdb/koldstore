//! Durable object_store put/get/publish tests.
//!
//! Covers atomic Create semantics, idempotent retry, size validation, reserved
//! staging-key rejection, and filesystem fsync-backed roundtrips.

use koldstore_storage::{
    backend_safe_publish_actions, open_filesystem_client, publish_immutable_object,
    publish_mutable_object, temp_object_key, unique_temp_file_name, validate_object_size,
    BackendConfig, ObjectStoreClient, PublishAction, PutPrecondition, StorageBackendKind,
    StorageClient, StorageClientError,
};
use serde_json::json;

#[test]
fn backend_config_validates_supported_storage_urls() {
    let filesystem = BackendConfig::new(
        StorageBackendKind::Filesystem,
        "file:///tmp/koldstore",
        json!({}),
    )
    .unwrap();
    assert_eq!(filesystem.base_path, "file:///tmp/koldstore");

    let s3 = BackendConfig::new(
        StorageBackendKind::S3,
        "s3://bucket/prefix",
        json!({"region": "us-east-1"}),
    )
    .unwrap();
    assert_eq!(s3.kind, StorageBackendKind::S3);

    assert!(BackendConfig::new(StorageBackendKind::S3, "/not/s3", json!({})).is_err());
}

#[test]
fn backend_safe_publish_actions_never_use_rename() {
    let actions = backend_safe_publish_actions(
        "scope/.tmp/writer/batch-0.parquet.tmp",
        "scope/batch-0.parquet",
        "scope/manifest.json",
    );

    assert_eq!(
        actions,
        vec![
            PublishAction::PutTemp("scope/.tmp/writer/batch-0.parquet.tmp".to_string()),
            PublishAction::CopyTempToFinal {
                temp: "scope/.tmp/writer/batch-0.parquet.tmp".to_string(),
                final_path: "scope/batch-0.parquet".to_string(),
            },
            PublishAction::ValidateFinal("scope/batch-0.parquet".to_string()),
            PublishAction::DeleteTemp("scope/.tmp/writer/batch-0.parquet.tmp".to_string()),
            PublishAction::PutManifest("scope/manifest.json".to_string()),
        ]
    );
}

#[test]
fn in_memory_put_get_delete_roundtrip() {
    let client = ObjectStoreClient::in_memory();
    let key = "app/items/batch-1.parquet";
    let payload = b"parquet-bytes-not-really";

    let put = client
        .put(key, payload, PutPrecondition::CreateIfAbsent)
        .unwrap();
    assert_eq!(put.byte_size, payload.len() as u64);
    assert_eq!(client.get(key).unwrap(), payload);

    let listed = client.list("app/items").unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, key);

    client.delete(key).unwrap();
    assert!(matches!(
        client.get(key),
        Err(StorageClientError::NotFound { .. })
    ));
    // Idempotent delete.
    client.delete(key).unwrap();
}

#[test]
fn create_if_absent_rejects_overwrite_but_overwrite_replaces() {
    let client = ObjectStoreClient::in_memory();
    let key = "seg/final.parquet";
    client
        .put(key, b"first", PutPrecondition::CreateIfAbsent)
        .unwrap();

    let conflict = client.put(key, b"second", PutPrecondition::CreateIfAbsent);
    assert!(matches!(
        conflict,
        Err(StorageClientError::AlreadyExists { .. })
    ));
    assert_eq!(client.get(key).unwrap(), b"first");

    client
        .put(key, b"second", PutPrecondition::Overwrite)
        .unwrap();
    assert_eq!(client.get(key).unwrap(), b"second");
}

#[test]
fn publish_immutable_is_idempotent_when_final_already_matches() {
    let client = ObjectStoreClient::in_memory();
    let bytes = b"complete-segment-bytes";
    let temp = temp_object_key(
        "app/items",
        "writer-a",
        &unique_temp_file_name("batch-0.parquet"),
    );
    let final_key = "app/items/batch-0.parquet";

    let first = publish_immutable_object(&client, &temp, final_key, bytes).unwrap();
    assert!(!first.reused_existing);
    assert_eq!(first.byte_size, bytes.len() as u64);
    assert!(matches!(
        client.get(&temp),
        Err(StorageClientError::NotFound { .. })
    ));

    let temp2 = temp_object_key(
        "app/items",
        "writer-b",
        &unique_temp_file_name("batch-0.parquet"),
    );
    let second = publish_immutable_object(&client, &temp2, final_key, bytes).unwrap();
    assert!(second.reused_existing);
    assert_eq!(client.get(final_key).unwrap(), bytes);
}

#[test]
fn publish_immutable_rejects_existing_final_with_mismatched_size() {
    let client = ObjectStoreClient::in_memory();
    client
        .put(
            "app/items/batch-1.parquet",
            b"short",
            PutPrecondition::CreateIfAbsent,
        )
        .unwrap();

    let temp = temp_object_key(
        "app/items",
        "writer",
        &unique_temp_file_name("batch-1.parquet"),
    );
    let err = publish_immutable_object(
        &client,
        &temp,
        "app/items/batch-1.parquet",
        b"much-longer-payload",
    )
    .unwrap_err();
    assert!(matches!(err, StorageClientError::Validation { .. }));
}

#[test]
fn publish_immutable_rejects_same_size_different_content() {
    let client = ObjectStoreClient::in_memory();
    client
        .put(
            "app/items/batch-2.parquet",
            b"AAAA",
            PutPrecondition::CreateIfAbsent,
        )
        .unwrap();

    let temp = temp_object_key(
        "app/items",
        "writer",
        &unique_temp_file_name("batch-2.parquet"),
    );
    let err =
        publish_immutable_object(&client, &temp, "app/items/batch-2.parquet", b"BBBB").unwrap_err();
    assert!(matches!(err, StorageClientError::Validation { .. }));
    // Corrupt/wrong final must remain untouched.
    assert_eq!(client.get("app/items/batch-2.parquet").unwrap(), b"AAAA");
}

#[test]
fn validate_object_size_detects_mismatch() {
    let client = ObjectStoreClient::in_memory();
    client
        .put("m/a.bin", b"abc", PutPrecondition::Overwrite)
        .unwrap();
    assert!(validate_object_size(&client, "m/a.bin", 3).is_ok());
    assert!(matches!(
        validate_object_size(&client, "m/a.bin", 99),
        Err(StorageClientError::Validation { .. })
    ));
}

#[test]
fn rejects_object_store_reserved_staging_key_suffix() {
    let client = ObjectStoreClient::in_memory();
    let err = client.put("foo.parquet#123", b"x", PutPrecondition::Overwrite);
    assert!(matches!(err, Err(StorageClientError::InvalidPath { .. })));
}

#[test]
fn filesystem_client_put_get_survives_reopen() {
    let root = tempfile::tempdir().unwrap();
    let client = open_filesystem_client(root.path().to_str().unwrap()).unwrap();
    let key = "ns/tbl/batch-9.parquet";
    let payload = b"durable-on-disk-bytes";

    publish_immutable_object(
        &client,
        &temp_object_key("ns/tbl", "w1", &unique_temp_file_name("batch-9.parquet")),
        key,
        payload,
    )
    .unwrap();

    // Re-open root and confirm the final object is visible (atomic publish).
    let client2 = open_filesystem_client(root.path().to_str().unwrap()).unwrap();
    assert_eq!(client2.get(key).unwrap(), payload);
    let abs = client2.absolute_path(key).unwrap();
    assert!(abs.is_file());
    assert_eq!(std::fs::read(&abs).unwrap(), payload);
}

#[test]
fn publish_mutable_manifest_overwrite_is_atomic_and_readable() {
    let client = ObjectStoreClient::in_memory();
    publish_mutable_object(&client, "ns/tbl/manifest.json", b"{\"v\":1}").unwrap();
    publish_mutable_object(&client, "ns/tbl/manifest.json", b"{\"v\":2}").unwrap();
    assert_eq!(client.get("ns/tbl/manifest.json").unwrap(), b"{\"v\":2}");
}

#[test]
fn list_prefix_combinations_return_only_matching_keys() {
    let client = ObjectStoreClient::in_memory();
    for key in [
        "a/t/batch-0.parquet",
        "a/t/batch-1.parquet",
        "a/u/batch-0.parquet",
        "b/t/batch-0.parquet",
    ] {
        client
            .put(key, b"x", PutPrecondition::CreateIfAbsent)
            .unwrap();
    }
    let a_t = client.list("a/t").unwrap();
    assert_eq!(a_t.len(), 2);
    let a = client.list("a").unwrap();
    assert_eq!(a.len(), 3);
}

#[test]
fn temp_object_key_avoids_hash_digit_staging_pattern() {
    let key = temp_object_key("app/items", "writer-1", "batch-0.parquet.abc.tmp");
    assert_eq!(key, "app/items/.tmp/writer-1/batch-0.parquet.abc.tmp");
    assert!(!key.split('/').any(|part| {
        part.rsplit_once('#').is_some_and(|(_, suffix)| {
            !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit())
        })
    }));
}
