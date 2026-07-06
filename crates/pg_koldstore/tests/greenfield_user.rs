use pg_koldstore::{
    migrate::{scope::plan_user_scope_policy, QualifiedTableName},
    sql::ddl::MigrateTableRequest,
};

fn user_request(scope_column: Option<&str>) -> MigrateTableRequest {
    MigrateTableRequest {
        table_name: "app.notes".to_string(),
        table_type: "user".to_string(),
        storage_name: "local-minio".to_string(),
        flush_policy: None,
        scope_column: scope_column.map(ToString::to_string),
        options: serde_json::json!({}),
    }
}

#[test]
fn user_greenfield_request_requires_existing_application_scope_column() {
    let request = user_request(None);

    assert!(request.has_supported_table_type());
    assert!(!request.has_valid_scope_arguments());
    assert_eq!(request.effective_scope_column(), None);
}

#[test]
fn user_greenfield_request_preserves_explicit_application_scope_column() {
    let request = user_request(Some("user_id"));

    assert!(request.has_valid_scope_arguments());
    assert_eq!(request.effective_scope_column(), Some("user_id"));
}

#[test]
fn user_scope_policy_uses_application_column_without_internal_user_id() {
    let table = QualifiedTableName::parse("app.notes").unwrap();
    let plan = plan_user_scope_policy(&table, "tenant_id").unwrap();
    let sql = plan
        .statements
        .iter()
        .map(|statement| statement.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(plan.scope_column, "tenant_id");
    assert!(sql.contains("\"tenant_id\" = current_setting('koldstore.user_id', true)"));
    assert!(!sql.contains("\"_user_id\""));
}
