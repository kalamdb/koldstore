#[test]
fn storage_rotation_contract_keeps_existing_object_paths_stable() {
    let registration = pg_koldstore::sql::ddl::StorageRegistration {
        name: "local-minio".to_string(),
        storage_type: "s3".to_string(),
        base_path: "s3://koldstore-test/".to_string(),
        credentials: serde_json::json!({"access_key_id": "old"}),
        config: serde_json::json!({"endpoint": "http://localhost:9000"}),
        shared_path_template: "{namespace}/{tableName}/".to_string(),
        user_path_template: "{namespace}/{tableName}/{scopeId}/".to_string(),
    };
    let old_path = registration.render_shared_prefix("app", "items").unwrap();
    let rotation = pg_koldstore::sql::ddl::alter_storage_credentials_plan(
        "local-minio",
        serde_json::json!({"access_key_id": "new"}),
    )
    .unwrap();

    assert_eq!(old_path, "app/items/");
    assert_eq!(rotation.storage_name, "local-minio");
    assert!(rotation.statement.sql.contains("SET credentials = $2"));
    assert!(!rotation.statement.sql.contains("base_path"));
}
