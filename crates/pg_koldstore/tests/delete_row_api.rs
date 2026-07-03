#[test]
fn sql_exposes_delete_row_tombstone_api() {
    use koldstore_core::TableName;
    use pg_koldstore::sql::dml::{plan_delete_row, DeleteInputState};

    let request = pg_koldstore::sql::dml::DeleteRowRequest {
        table_name: TableName::parse("app.items").unwrap(),
        pk_json: serde_json::json!({"id": 1}),
        allow_may_contain: pg_koldstore::sql::dml::DeleteRowRequest::DEFAULT_ALLOW_MAY_CONTAIN,
    };

    assert!(pg_koldstore::sql::dml::COLD_DML_FUNCTIONS.contains(&"koldstore.delete_row"));
    assert!(request.allow_may_contain);
    assert_eq!(
        pg_koldstore::sql::dml::delete_decision(true),
        pg_koldstore::sql::dml::DeleteDecision::Tombstone
    );

    let result = plan_delete_row(&request, DeleteInputState::ColdExactLocalHint);
    assert_eq!(result.affected_rows, 1);
    assert!(result.tombstone_written);
    assert!(!result.cold_lookup_performed);
}
