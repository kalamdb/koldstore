#[test]
fn shared_greenfield_request_uses_no_scope_column() {
    let request = koldstore_migrate::MigrateTableRequest {
        table_name: koldstore_common::TableName::parse("app.shared_items").unwrap(),
        table_type: "shared".to_string(),
        storage_name: "local-minio".to_string(),
        scope_column: None,
        options: koldstore_common::ManageTableOptions::default().with_flush(1000, 1, 1000),
    };

    assert!(request.has_supported_table_type());
    assert!(request.has_valid_scope_arguments());
    assert_eq!(request.effective_scope_column(), None);
}
