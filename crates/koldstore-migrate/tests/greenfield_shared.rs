#[test]
fn shared_greenfield_request_uses_no_scope_column() {
    let request = koldstore_migrate::MigrateTableRequest {
        table_name: "app.shared_items".to_string(),
        table_type: "shared".to_string(),
        storage_name: "local-minio".to_string(),
        scope_column: None,
        options: koldstore_common::ManageTableOptions::from_value(
            &serde_json::json!({ "hot_row_limit": 1000 }),
        ),
    };

    assert!(request.has_supported_table_type());
    assert!(request.has_valid_scope_arguments());
    assert_eq!(request.effective_scope_column(), None);
}
