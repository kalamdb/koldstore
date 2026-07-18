//! Opt-in MinIO integration tests.

#![cfg(feature = "s3")]

use koldstore_storage::{
    open_client_from_catalog_fields, publish_immutable_object, publish_mutable_object,
    temp_object_key, ObjectStoreClient, PutPrecondition, StorageClient, StorageClientError,
};
use serde_json::json;

fn minio_client() -> Option<ObjectStoreClient> {
    let enabled = std::env::var("KOLDSTORE_MINIO").ok().as_deref() == Some("1");
    let endpoint = std::env::var("KOLDSTORE_MINIO_ENDPOINT").ok();
    if !enabled && endpoint.is_none() {
        return None;
    }
    let endpoint = endpoint.unwrap_or_else(|| "http://127.0.0.1:19000".to_string());
    let access_key =
        std::env::var("KOLDSTORE_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key =
        std::env::var("KOLDSTORE_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let bucket =
        std::env::var("KOLDSTORE_MINIO_BUCKET").unwrap_or_else(|_| "koldstore-test".to_string());
    Some(
        open_client_from_catalog_fields(
            "s3",
            &format!("s3://{bucket}"),
            &json!({
                "access_key_id": access_key,
                "secret_access_key": secret_key,
            }),
            &json!({
                "endpoint": endpoint,
                "region": "us-east-1",
                "path_style": true,
            }),
        )
        .expect("open MinIO client"),
    )
}

fn prefix() -> String {
    format!("integration/{}", uuid::Uuid::new_v4())
}

#[test]
fn minio_put_get_list_delete_roundtrip() {
    let Some(client) = minio_client() else {
        return;
    };
    let prefix = prefix();
    let key = format!("{prefix}/object.bin");
    client
        .put(&key, b"payload", PutPrecondition::CreateIfAbsent)
        .unwrap();
    assert_eq!(client.get(&key).unwrap(), b"payload");
    assert_eq!(client.list(&prefix).unwrap().len(), 1);
    client.delete(&key).unwrap();
    assert!(matches!(
        client.get(&key),
        Err(StorageClientError::NotFound { .. })
    ));
}

#[test]
fn minio_immutable_publish_is_idempotent_and_rejects_mismatch() {
    let Some(client) = minio_client() else {
        return;
    };
    let prefix = prefix();
    let final_key = format!("{prefix}/batch-0.parquet");
    let first_temp = temp_object_key(&prefix, "writer-1", "batch-0.parquet.tmp");
    let first = publish_immutable_object(&client, &first_temp, &final_key, b"same").unwrap();
    assert!(!first.reused_existing);
    let second_temp = temp_object_key(&prefix, "writer-2", "batch-0.parquet.tmp");
    let second = publish_immutable_object(&client, &second_temp, &final_key, b"same").unwrap();
    assert!(second.reused_existing);
    let mismatch_temp = temp_object_key(&prefix, "writer-3", "batch-0.parquet.tmp");
    assert!(matches!(
        publish_immutable_object(&client, &mismatch_temp, &final_key, b"diff"),
        Err(StorageClientError::Validation { .. })
    ));
    client.delete(&final_key).unwrap();
}

#[test]
fn minio_mutable_publish_overwrites() {
    let Some(client) = minio_client() else {
        return;
    };
    let key = format!("{}/manifest.json", prefix());
    publish_mutable_object(&client, &key, b"{\"v\":1}").unwrap();
    publish_mutable_object(&client, &key, b"{\"v\":2}").unwrap();
    assert_eq!(client.get(&key).unwrap(), b"{\"v\":2}");
    client.delete(&key).unwrap();
}

#[test]
fn minio_recovery_quarantines_final_and_deletes_temp() {
    let Some(client) = minio_client() else {
        return;
    };
    let prefix = prefix();
    let temp = format!("{prefix}/.tmp/writer/segment.tmp");
    let final_key = format!("{prefix}/batch-9.parquet");
    client
        .put(&temp, b"temporary", PutPrecondition::Overwrite)
        .unwrap();
    client
        .put(&final_key, b"orphan", PutPrecondition::Overwrite)
        .unwrap();

    client.delete(&temp).unwrap();
    let quarantine = format!("{final_key}.quarantine.{}", uuid::Uuid::new_v4());
    let bytes = client.get(&final_key).unwrap();
    client
        .put(&quarantine, &bytes, PutPrecondition::CreateIfAbsent)
        .unwrap();
    client.delete(&final_key).unwrap();

    assert_eq!(client.get(&quarantine).unwrap(), b"orphan");
    assert!(matches!(
        client.get(&final_key),
        Err(StorageClientError::NotFound { .. })
    ));
    client.delete(&quarantine).unwrap();
}
