use koldstore_storage::{
    backend_safe_publish_actions, BackendConfig, ConditionalPut, PathTemplate, PublishAction,
    StorageBackendKind,
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
            PublishAction::DeleteTemp("scope/.tmp/writer/batch-0.parquet.tmp".to_string()),
            PublishAction::PutManifest("scope/manifest.json".to_string()),
        ]
    );
    assert!(actions
        .iter()
        .all(|action| !matches!(action, PublishAction::Rename { .. })));
}

#[test]
fn path_template_rejects_unresolved_placeholders() {
    let template = PathTemplate::new("{namespace}/{tableName}/{unknown}/");
    assert!(template.render("app", "items", None).is_err());
}

#[test]
fn conditional_put_descriptions_are_stable() {
    assert_eq!(
        ConditionalPut::IfAbsent.description(),
        "put only when target is absent"
    );
    assert_eq!(
        ConditionalPut::IfMatch.description(),
        "put only when identity matches"
    );
}
